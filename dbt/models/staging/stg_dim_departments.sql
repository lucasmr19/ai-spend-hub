select * from {{ source('raw', 'dim_departments') }}
