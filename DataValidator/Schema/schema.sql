create table table_count_compare (
tbl_id        SERIAL,
database_name VARCHAR,
tablename     VARCHAR,
record_count  VARCHAR);

drop table metaschema.scd_table_metadata;
create table metaschema.scd_table_metadata (
tbl_id        INTEGER,
database_name VARCHAR,
tbl_name      VARCHAR,
col_name      VARCHAR,
col_type      VARCHAR,
col_length    VARCHAR,
col_scale     VARCHAR);


CREATE SEQUENCE metaschema.table_meta_seq;