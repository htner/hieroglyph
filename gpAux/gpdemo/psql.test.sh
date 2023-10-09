export PGOPTIONS='-c default_table_access_method=parquet'
psql -d test.postgres
