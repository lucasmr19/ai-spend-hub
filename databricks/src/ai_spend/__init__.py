"""Portable PySpark logic for the AI Spend & Reliability Hub.

DataFrame / SQL API only (no RDDs, no SparkContext) so the same code runs on a local
Spark session (tests), Databricks serverless (Spark Connect) and classic clusters.
"""
