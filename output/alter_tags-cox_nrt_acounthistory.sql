-- DEV
ALTER TABLE wde_dev.cu_b.cox_nrt_acounthistory ALTER COLUMN msisdn SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde_dev.cu_b.cox_nrt_acounthistory ALTER COLUMN imei SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde_dev.cu_b.cox_nrt_acounthistory ALTER COLUMN owning_subscriber_id SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde_dev.cu_b.cox_nrt_acounthistory ALTER COLUMN imsi SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde_dev.cu_b.cox_nrt_acounthistory ALTER COLUMN enodebd SET TAGS ('infoclass' = 'restricted');

-- PET
ALTER TABLE wde_pet.cu_b.cox_nrt_acounthistory ALTER COLUMN msisdn SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde_pet.cu_b.cox_nrt_acounthistory ALTER COLUMN imei SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde_pet.cu_b.cox_nrt_acounthistory ALTER COLUMN owning_subscriber_id SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde_pet.cu_b.cox_nrt_acounthistory ALTER COLUMN imsi SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde_pet.cu_b.cox_nrt_acounthistory ALTER COLUMN enodebd SET TAGS ('infoclass' = 'restricted');

-- BASE
ALTER TABLE wde.cu_b.cox_nrt_acounthistory ALTER COLUMN msisdn SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde.cu_b.cox_nrt_acounthistory ALTER COLUMN imei SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde.cu_b.cox_nrt_acounthistory ALTER COLUMN owning_subscriber_id SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde.cu_b.cox_nrt_acounthistory ALTER COLUMN imsi SET TAGS ('dataprivacy' = 'personal');
ALTER TABLE wde.cu_b.cox_nrt_acounthistory ALTER COLUMN enodebd SET TAGS ('infoclass' = 'restricted');