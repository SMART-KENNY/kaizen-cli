CREATE TABLE IF NOT EXISTS wde_dev.cu_b.opt_dly_opt_workorder_item (
    workorder_item_id decimal(19,0) comment '',
    address_action_id string comment '',
    comments string comment '',
    site_disc decimal(19,0) comment '',
    fulfillment_partner_id decimal(10,0) comment '',
    installation_time string comment '',
    owning_account_id string comment '',
    owning_cfs_id string comment '',
    work_fulfillment_options string comment '',
    workorder_item_external_id string comment '',
    work_specification_external_id string comment '',
    work_specification_id decimal(19,0) comment '',
    order_id decimal(19,0) comment '',
    workfulfillmentoptionsretrievalerror string comment '',
    sid_source string comment '',
    file_date date comment 'date inline from the file',
    dbx_process_dttm timestamp comment 'records the date and time when the data was processed in the dbx system, important for tracking data processing activities',
    file_name string comment 'contains the name of the file from which the data was derived, aiding in data traceability and management'
) 
USING delta
PARTITIONED BY (file_date)
COMMENT ''
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'retentionKey' = 'file_date'
);