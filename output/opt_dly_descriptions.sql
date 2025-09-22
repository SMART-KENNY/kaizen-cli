select
 cast(description_code as double(10,0)) description_code,
 cast(language_code as double(6,0)) language_code,
 cast(description_group as double(6,0)) description_group,
 cast(description_text as string) description_text,
 cast(short_description_text as string) short_description_text,
 cast(config_tag_id as string) config_tag_id,
 cast(tenant_id as double(10,0)) tenant_id,
 cast(sid_source as string) sid_source,
 process_dt,
 load_date,
 now() dbx_process_dttm
from opt_dly_descriptions