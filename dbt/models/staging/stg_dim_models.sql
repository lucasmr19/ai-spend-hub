select * from {{ source('raw', 'dim_models') }}
