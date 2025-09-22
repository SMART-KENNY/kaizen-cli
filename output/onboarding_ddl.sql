CREATE TABLE IF NOT EXISTS wde_dev.cu_b.opt_dly_descriptions (
    description_code double(10,0) comment '',
    language_code double(6,0) comment '',
    description_group double(6,0) comment '',
    description_text string comment '',
    short_description_text string comment '',
    config_tag_id string comment '',
    tenant_id double(10,0) comment '',
    sid_source string comment '',
    process_dt date comment 'dd-mm-yyyy',
    load_date date comment 'date inline from the file',
    dbx_process_dttm timestamp comment 'records the date and time when the data was processed in the dbx system, important for tracking data processing activities'
) 
USING delta
PARTITIONED BY (txn_date, file_date)
COMMENT ''
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'retentionKey' = 'txn_date'
);