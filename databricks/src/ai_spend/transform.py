"""Bronze -> Silver logic: validation, dedupe, late-data routing, canonicalisation.

Spark equivalent of ``contracts/validate.py`` + ``reference/pipeline.py``. A test checks that
the reason code of every landed line is identical to the pure-Python oracle.

Bronze schema (one row per landed line)::

    raw STRING, source_file STRING, arrival_date DATE
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

WATERMARK_DAYS = 7

REASONS = {
    "E000": "schema_invalid",
    "E001": "missing_event_id",
    "E002": "negative_tokens",
    "E003": "wrong_type_tokens",
    "E004": "missing_latency",
    "E005": "unsupported_version",
    "E101": "unknown_model",
    "E102": "app_department_mismatch",
    "E103": "future_timestamp",
}

# Permissive union of v1 and v2: type-checking of tokens is done on the raw text (Spark would
# silently coerce the JSON string "5" to 5).
PARSE_SCHEMA = (
    "schema_version INT, event_id STRING, event_ts STRING, app_id STRING, department_id STRING, "
    "model_id STRING, user_id STRING, request_type STRING, tokens_in LONG, tokens_out LONG, "
    "latency_ms LONG, latency_total_ms LONG, ttft_ms LONG, cache_hit BOOLEAN, cache_status STRING, "
    "status STRING, error_code STRING"
)
_COMMON = ["schema_version", "event_id", "event_ts", "app_id", "department_id", "model_id", "user_id",
           "tokens_in", "tokens_out", "status", "error_code"]
ALLOWED_V1 = _COMMON + ["latency_ms", "cache_hit"]
ALLOWED_V2 = _COMMON + ["request_type", "latency_total_ms", "ttft_ms", "cache_status"]

_UUID = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
_TS = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
_USER = r"^u-[0-9a-f]{8}$"
_REQUEST_TYPES = ["qa_rag", "extraction", "drafting", "code", "summarization", "analysis"]


def _b(col) -> F.Column:
    """Null-safe boolean: a NULL condition counts as 'not violated'."""
    return F.coalesce(col, F.lit(False))


# --------------------------------------------------------------------------- dimensions
def load_dimensions(spark: SparkSession, dims_dir: str) -> dict[str, DataFrame]:
    """Load the dimension CSVs (header, all strings) and type them."""
    def rd(name: str) -> DataFrame:
        return spark.read.option("header", "true").csv(f"{dims_dir.rstrip('/')}/{name}.csv")

    centi = lambda c: F.round(F.col(c).cast("decimal(10,2)") * 100).cast("int")  # noqa: E731
    return {
        "departments": rd("departments"),
        "apps": rd("apps").withColumn("cache_enabled", F.col("cache_enabled") == "true"),
        "models": rd("models"),
        "model_prices": rd("model_prices").select(
            "model_id",
            F.to_date("valid_from").alias("valid_from"),
            F.to_date(F.when(F.col("valid_to") == "", None).otherwise(F.col("valid_to"))).alias("valid_to"),
            centi("price_in_eur_per_mtok").alias("price_in_centi"),
            centi("price_out_eur_per_mtok").alias("price_out_centi"),
        ),
        "slo_targets": rd("slo_targets").select(
            "app_id", F.col("target_availability_pct").cast("double").alias("target_availability_pct")),
        "model_quality": rd("model_quality").select(
            "model_id", "request_type", F.col("quality_score").cast("double").alias("quality_score"),
            F.col("eval_samples").cast("int").alias("eval_samples")),
        "budgets": rd("budgets").select(
            "department_id", "month", F.col("budget_eur").cast("decimal(12,2)").alias("budget_eur")),
    }


# --------------------------------------------------------------------------- bronze (local)
def read_bronze_local(spark: SparkSession, landing_dir: str) -> DataFrame:
    """Same schema Auto Loader produces in Databricks, for local runs and tests."""
    return (
        spark.read.text(landing_dir, recursiveFileLookup=True)
        .select(F.col("value").alias("raw"), F.col("_metadata.file_path").alias("source_file"))
        .withColumn("arrival_date", arrival_date_from_path(F.col("source_file")))
    )


def arrival_date_from_path(path_col: F.Column) -> F.Column:
    return F.to_date(F.regexp_extract(path_col, r"dt=(\d{4}-\d{2}-\d{2})", 1))


# --------------------------------------------------------------------------- validation
def validate(bronze: DataFrame, dims: dict[str, DataFrame]) -> DataFrame:
    """Parse every line and attach ``reason`` (NULL when the record is valid)."""
    models = dims["models"].select("model_id", F.lit(True).alias("_model_known"))
    apps = dims["apps"].select(
        "app_id", F.col("department_id").alias("_app_dept"), F.col("default_request_type").alias("_default_rt"))

    parsed = bronze.withColumn("_p", F.from_json("raw", PARSE_SCHEMA)).select("raw", "source_file", "arrival_date", "_p.*")
    keys = F.map_keys(F.from_json(F.col("raw"), "map<string,string>"))
    v = F.col("schema_version")
    allowed = F.when(v == 1, F.array(*[F.lit(k) for k in ALLOWED_V1])).otherwise(
        F.array(*[F.lit(k) for k in ALLOWED_V2]))

    latency_missing = F.when(v == 1, F.col("latency_ms").isNull()).otherwise(F.col("latency_total_ms").isNull())
    v1_missing = F.col("cache_hit").isNull()
    v2_missing = F.col("request_type").isNull() | F.col("cache_status").isNull()
    other_required_missing = (
        F.col("event_ts").isNull() | F.col("app_id").isNull() | F.col("department_id").isNull()
        | F.col("model_id").isNull() | F.col("user_id").isNull() | F.col("status").isNull()
        | F.col("tokens_in").isNull() | F.col("tokens_out").isNull()
        | F.when(v == 1, v1_missing).otherwise(v2_missing)
    )
    bad_format = (
        ~_b(F.col("event_id").rlike(_UUID)) | ~_b(F.col("event_ts").rlike(_TS))
        | ~_b(F.col("user_id").rlike(_USER)) | ~_b(F.col("status").isin("ok", "error", "timeout"))
        | _b((v == 2) & (~_b(F.col("cache_status").isin("hit", "miss", "bypass"))
                         | ~_b(F.col("request_type").isin(*_REQUEST_TYPES))))
    )
    extra_keys = F.size(F.array_except(keys, allowed)) > 0
    string_tokens = F.col("raw").rlike(r'"tokens_(in|out)":"')

    reason = (
        F.when(~_b(v.isin(1, 2)), "E005")
        .when(F.col("event_id").isNull(), "E001")
        .when(_b(latency_missing), "E004")
        .when(_b((F.col("tokens_in") < 0) | (F.col("tokens_out") < 0)), "E002")
        .when(string_tokens, "E003")
        .when(_b(other_required_missing) | _b(bad_format) | _b(extra_keys), "E000")
        .when(~_b(F.col("_model_known")), "E101")
        .when(~_b(F.col("_app_dept") == F.col("department_id")), "E102")
        .when(F.to_date(F.substring("event_ts", 1, 10)) > F.col("arrival_date"), "E103")
    )
    return (
        parsed.join(F.broadcast(models), "model_id", "left")
        .join(F.broadcast(apps), "app_id", "left")
        .withColumn("reason", reason)
        .drop("_model_known")
    )


# --------------------------------------------------------------------------- classification
def classify_lines(bronze: DataFrame, dims: dict[str, DataFrame], watermark_days: int = WATERMARK_DAYS) -> DataFrame:
    """One row per landed line with a ``disposition``:

    quarantine       invalid record (``reason`` says why)
    duplicate        valid, but an earlier arrival of the same event_id exists (first arrival wins)
    late_quarantine  first arrival, but more than ``watermark_days`` after the event date
    silver           first arrival, within the watermark

    Duplicates are resolved *before* the watermark rule: a replayed copy of an event that is
    already in Silver is a duplicate, not a late event.
    """
    v = validate(bronze, dims)
    key = F.when(F.col("reason").isNull(), F.col("event_id")).otherwise(
        F.concat(F.lit("invalid:"), F.sha2(F.concat_ws("|", "source_file", "raw"), 256)))
    w = Window.partitionBy(key).orderBy("arrival_date", "source_file")
    event_date = F.to_date(F.substring("event_ts", 1, 10))
    return (
        v.withColumn("_rn", F.row_number().over(w))
        .withColumn("event_date", F.when(F.col("reason").isNull(), event_date))
        .withColumn("lag_days", F.datediff("arrival_date", "event_date"))
        .withColumn(
            "disposition",
            F.when(F.col("reason").isNotNull(), "quarantine")
            .when(F.col("_rn") > 1, "duplicate")
            .when(F.col("lag_days") > watermark_days, "late_quarantine")
            .otherwise("silver"),
        )
        .drop("_rn")
    )


def canonicalize(classified: DataFrame, disposition: str = "silver") -> DataFrame:
    """Canonical Silver shape (v2 + source_schema_version) for one disposition."""
    v = F.col("schema_version")
    return classified.filter(F.col("disposition") == disposition).select(
        "event_id",
        F.to_timestamp("event_ts", "yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("event_ts"),
        "event_date",
        "app_id", "department_id", "model_id", "user_id",
        F.when(v == 2, F.col("request_type")).otherwise(F.col("_default_rt")).alias("request_type"),
        "tokens_in", "tokens_out",
        F.when(v == 2, F.col("latency_total_ms")).otherwise(F.col("latency_ms")).alias("latency_total_ms"),
        F.when(v == 2, F.col("ttft_ms")).alias("ttft_ms"),
        F.when(v == 2, F.col("cache_status")).otherwise(
            F.when(F.col("cache_hit"), "hit").otherwise("miss")).alias("cache_status"),
        "status", "error_code",
        v.alias("source_schema_version"),
        "arrival_date", "lag_days", "source_file",
    )


def quarantine_rows(classified: DataFrame) -> DataFrame:
    """Rejected records with their reason; ``quarantine_key`` makes MERGE idempotent."""
    return classified.filter(F.col("disposition") == "quarantine").select(
        F.sha2(F.concat_ws("|", "source_file", "raw"), 256).alias("quarantine_key"),
        "raw", "source_file", "arrival_date", F.col("reason").alias("reason_code"),
    )
