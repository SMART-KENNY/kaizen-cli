CREATE TABLE IF NOT EXISTS wde_dev.cu_b.cox_nrt_acounthistory (
    msisdn string COMMENT 'unknown',
    historytypekeya string COMMENT 'unknown',
    reasonfordeletion string COMMENT 'unknown',
    tstamp timestamp COMMENT 'timestamp',
    username string COMMENT 'unknown',
    accountnum longinteger COMMENT 'unknown',
    additionalinfo string COMMENT 'unknown',
    propertytypename string COMMENT 'unknown',
    source string COMMENT 'unknown',
    externaltransactionreference string COMMENT 'unknown',
    subscriptionproperty string COMMENT 'unknown',
    imei date COMMENT 'date',
    asd date COMMENT 'date',
    asd timestamp COMMENT 'timestamp',
    owning_subscriber_id string COMMENT 'unknown',
    imsi string COMMENT 'unknown',
    txn_dt date COMMENT 'date',
    asd date COMMENT 'date',
    enodebd timestamp COMMENT 'timestamp',
    asd string COMMENT 'unknown',
    asd string COMMENT 'unknown',
    txn_dt date COMMENT 'date',
    file_date date COMMENT 'date',
    dbx_process_dttm timestamp COMMENT 'timestamp',
    file_name string COMMENT 'unknown'
) 
USING delta
PARTITIONED BY (txn_date, file_date)
COMMENT ''
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'retentionKey' = 'txn_date'
);