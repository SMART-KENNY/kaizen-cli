import os
import math
import pandas as pd
import json
from dotenv import load_dotenv
from dataclasses import dataclass, fields
from typing import Optional
from pathlib import Path
import re

from components.c_cluster_compute import get_cluster_values



@dataclass
class ContextParam:
    p_work_space: Optional[str] = None
    tier_suffix: Optional[str] = None
    p_pipeline: Optional[str] = None
    p_tier: Optional[str] = None
    p_table_name: Optional[str] = None
    p_data_domain: Optional[str] = None
    p_frequency: Optional[str] = None
    p_soure_file_format: Optional[str] = None
    p_header: Optional[str] = None
    p_delimeter: Optional[str] = ""
    p_save_mode: Optional[str] = None
    p_source_directory: Optional[str] = None
    on_prem_p_file_mask: Optional[str] = None
    p_file_size: Optional[str] = None
    p_application_name: Optional[str] = None
    p_task_key_name: Optional[str] = None
    p_partition_column: Optional[str] = None
    p_retention_key: Optional[str] = None
    s3_source_file_dir: Optional[str] = None
    s3_source_file_mask: Optional[str] = None
    s3_manifest_file_mask: Optional[str] = None


    p_project_name: Optional[str] = None
    p_jobs_to_deploy: Optional[str] = None
    p_dev: Optional[str] = None
    p_implementation_date: Optional[str] = None




def generate_job_yml(
    input_path,
    output_path,
    replacements: dict[str, str],) -> None:

    try:
        input_path = Path(input_path)
        output_path = Path(output_path)

        content = input_path.read_text()
        for target, replacement in replacements.items():
            content = re.sub(re.escape(target), replacement, content)
        output_path.write_text(content)
        print(f"✅ JOB YML: {output_path.name}")
    
    except FileNotFoundError:
        print(f"❌ Template file not found: {input_path}")
    except PermissionError:
        print(f"❌ Permission denied when writing to: {output_path}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def dev_generate_json_config_data_sync(
    input_path: Path,
    output_path: Path,
    replacements: dict[str, str],) -> None:

    try:
        content = input_path.read_text()
        for target, replacement in replacements.items():
            content = content.replace(target, replacement)
        output_path.write_text(content)
        print(f"✅ DATA-SYNC JSON FILE: {output_path.name}")
    
    except FileNotFoundError:
        print(f"❌ Template file not found: {input_path}")
    except PermissionError:
        print(f"❌ Permission denied when writing to: {output_path}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

def pet_generate_json_config_data_sync(
    input_path: Path,
    output_path: Path,
    replacements: dict[str, str],) -> None:

    try:
        content = input_path.read_text()
        for target, replacement in replacements.items():
            content = content.replace(target, replacement)
        output_path.write_text(content)
        print(f"✅ DATA-SYNC JSON FILE: {output_path.name}")
    
    except FileNotFoundError:
        print(f"❌ Template file not found: {input_path}")
    except PermissionError:
        print(f"❌ Permission denied when writing to: {output_path}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

def data_sync_runner(
    input_path: Path,
    output_path: Path,
    replacements: dict[str, str],) -> None:

    try:
        content = input_path.read_text(encoding="utf-8")
        for target, replacement in replacements.items():
            content = content.replace(target, replacement)
        output_path.write_text(content, encoding="utf-8")
        print(f"✅ DATA-SYNC RUNNER SCRIPT FILE: {output_path.name}")
    
    except FileNotFoundError:
        print(f"❌ Template file not found: {input_path}")
    except PermissionError:
        print(f"❌ Permission denied when writing to: {output_path}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def generate_grants(
    input_path: Path,
    output_path: Path,
    replacements: dict[str, str],) -> None:

    try:
        content = input_path.read_text()
        for target, replacement in replacements.items():
            content = content.replace(target, replacement)
        output_path.write_text(content)
        print(f"✅ ONBOARDING GRANTS: {output_path.name}")
    
    except FileNotFoundError:
        print(f"❌ Template file not found: {input_path}")
    except PermissionError:
        print(f"❌ Permission denied when writing to: {output_path}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def generate_instructions(
    input_path: Path,
    output_path: Path,
    replacements: dict[str, str],) -> None:

    try:
        content = input_path.read_text()
        for target, replacement in replacements.items():
            content = content.replace(target, replacement)
        output_path.write_text(content)
        print(f"✅ INSTRUCTIONS GENERATED: {output_path.name}")
    
    except FileNotFoundError:
        print(f"❌ Template file not found: {input_path}")
    except PermissionError:
        print(f"❌ Permission denied when writing to: {output_path}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")



def generate_sql(
    file_path: str = 'dbx_context_param_optima.xlsx',
    sheet: str = 'schema',
    table_name: str = '',
    output_file: str = '',
    p_pipeline: str = '',
    p_tier: str = '',
    p_header: str = ''
) -> None:
    df = pd.read_excel(file_path, sheet_name=sheet)
    df = df.iloc[:, 0:3]  # Columns A (iterator), B (field), C (type)

    lines = []

    if p_header == "true":
        for _, row in df.iterrows():
            iterator = str(row.iloc[0])
            field_name = str(row.iloc[1])
            data_type = str(row.iloc[2]) if not pd.isna(row.iloc[2]) else ""

            if field_name == "dbx_process_dttm":
                line = f"now() as {field_name}"
            # elif field_name == "process_dt":
            #     line = f"now() process_date"
            # elif field_name == "load_date":
            #     line = f"to_date(date_format(now(), 'yyyy-MM-dd'), 'yyyy-MM-dd') as load_date"
            elif field_name == "file_date":
                line = f"to_date(date_format(now(), 'yyyy-mm-dd'), 'yyyy-mm-dd') as {field_name}"
            elif field_name == "sid_source":
                line = f"'SID' as {field_name}"
            elif data_type.lower() in {"timestamp", "date"} or field_name in {"file_name", "file_id", "owning_subscriber_id", "msisdn","subscriber_id", "sub_id","ret_min","ret_msisdn","dsp_min","dealer_min"}:
                line = f"{field_name}"
            else:
                line = f"cast({field_name} as {data_type}) {field_name}"
        
            lines.append(line.lower())

    else:
        for _, row in df.iterrows():
            iterator = str(row.iloc[0])
            field_name = str(row.iloc[1])
            data_type = str(row.iloc[2]) if not pd.isna(row.iloc[2]) else ""

            if field_name == "dbx_process_dttm":
                line = f"now() as {field_name}"
            # elif field_name == "process_dt":
            #     line = f"now() process_date"
            # elif field_name == "load_date":
            #     line = f"to_date(date_format(now(), 'yyyy-MM-dd'), 'yyyy-MM-dd') as load_date"
            elif field_name == "file_date":
                line = f"to_date(date_format(now(), 'yyyy-mm-dd'), 'yyyy-mm-dd') as {field_name}"
            elif field_name == "sid_source":
                line = f"'SID' as {field_name}"
            elif data_type.lower() in {"timestamp", "date"} or field_name in {"file_name", "file_id", "owning_subscriber_id", "msisdn","subscriber_id", "sub_id","ret_min","ret_msisdn","dsp_min","dealer_min"}:
                line = f"{field_name}"
            else:
                line = f"cast({iterator} as {data_type}) {field_name}"
            
            lines.append(line.lower())

    sql_lines = []
    sql_lines.append("select")
    for i, line in enumerate(lines):
        suffix = "," if i < len(lines) - 1 else ""
        sql_lines.append(f" {line}{suffix}")
    sql_lines.append(f"from {table_name}")

    if not output_file:
        output_file = "generated"

    output_path = Path(f"{output_file}{p_pipeline}.sql")
    output_path.write_text("\n".join(sql_lines), encoding="utf-8")

    print(f"✅ SQL FILE {output_path.resolve()}")



def generate_table_tags(
    file_path: str = "dbx_context_param_optima.xlsx",
    sheet: str = "schema",
    table_name: str = "",
    output_file: str | Path = "tags_output.sql",
    work_space: str = "",
    data_domain: str = "",
    tier_suffix: str = "",
) -> None:
    """
    Generate ALTER TABLE statements with tags based on Excel metadata,
    separated by environment (DEV, PET, BASE).
    """
    df = pd.read_excel(file_path, sheet_name=sheet).iloc[:, 0:5]

    # map tag_key -> (tag_name, tag_value)
    tag_mapping = {
        "dataprivacy": ("dataprivacy", "personal"),
        "infoclass": ("infoclass", "restricted"),
    }

    # environments: (label, schema_prefix)
    environments = [
        ("DEV", f"{work_space}_dev"),
        ("PET", f"{work_space}_pet"),
        ("BASE", work_space),
    ]

    sections = []

    for env_label, env_prefix in environments:
        lines = [f"-- {env_label}"]
        for _, row in df.iterrows():
            field_name = str(row.iloc[1]).strip()
            tag_key = str(row.iloc[4]).lower().strip()

            if tag_key not in tag_mapping:
                continue

            tag_name, tag_value = tag_mapping[tag_key]
            schema = f"{env_prefix}.{data_domain}{tier_suffix}"

            line = (
                f"ALTER TABLE {schema}.{table_name} "
                f"ALTER COLUMN {field_name} "
                f"SET TAGS ('{tag_name}' = '{tag_value}');"
            )
            lines.append(line)
        sections.append("\n".join(lines))

    # Write output with blank line between sections
    output_path = Path(output_file)
    output_path.write_text("\n\n".join(sections), encoding="utf-8")

    print(f"✅ TAGS FILE CREATED: {output_path.resolve()}")




def generate_onboarding_ddl(
    file_path: str = 'dbx_context_param_optima.xlsx',
    sheet: str = 'schema',
    table_name: str = "",
    work_space: str = "",
    data_domain: str = "",
    tier_suffix: str = "",
    p_partition_column: str = "",
    p_retention_key: str = "",
    output_file: str = ""
) -> None:
    if not table_name or not work_space or not data_domain:
        raise ValueError("table_name, work_space, and data_domain are required.")

    df = pd.read_excel(file_path, sheet_name=sheet)
    df = df.iloc[:, 0:4]

    lines = []

    for _, row in df.iterrows():
        field_name = str(row.iloc[1]).strip()
        data_type = str(row.iloc[2]).strip()
        comment = str(row.iloc[3]).strip() if not pd.isna(row.iloc[3]) else ""
        comment = comment.replace("'", "''")  # Escape single quotes

        line = f"{field_name} {data_type} COMMENT '{comment}'"
        lines.append(line.lower())


    last_valid_index = max(i for i, l in enumerate(lines) if "file_id" not in l)
    sql_lines = []
    sql_lines.append(f"CREATE TABLE IF NOT EXISTS {work_space}_dev.{data_domain}{tier_suffix}.{table_name} (")
    for i, line in enumerate(lines):
        if "file_id" in line:
            continue
        suffix = "," if i < last_valid_index else ""
        sql_lines.append(f"    {line}{suffix}")
    
    sql_lines.append(") ")
    sql_lines.append("USING delta")
    sql_lines.append(f"PARTITIONED BY ({p_partition_column})")
    sql_lines.append("COMMENT ''")
    sql_lines.append("TBLPROPERTIES (")
    sql_lines.append("    'delta.enableChangeDataFeed' = 'true',")
    sql_lines.append(f"    'retentionKey' = '{p_retention_key}'")
    sql_lines.append(");")

    output_path = Path(f"{output_file}onboarding_ddl.sql")
    output_path.write_text("\n".join(sql_lines), encoding="utf-8")

    print(f"✅ ONBOARDING DDL {output_path.resolve()}")




def generate_json_config(
    file_path: str = 'dbx_context_param_optima.xlsx',
    sheet: str = 'schema',
    output_file: str = "",
    pipe_line: str = "",
    template_path: str = "",) -> None:


    df = pd.read_excel(file_path, sheet_name=sheet)
    df = df.iloc[:, 0:4]

    columns = []

    for _, row in df.iterrows():
        field_name = str(row.iloc[1]).strip()
        data_type = str(row.iloc[2]).strip()

        if not field_name or not data_type:
            continue
        
        # if field_name == "process_dt":
        #     columns.append({
        #         "column_name": "process_date",
        #         "data_type": data_type.lower()
        #     })
        # else:
        columns.append({
            "column_name": field_name.lower(),
            "data_type": data_type.lower()
        })


    pretty_json = json.dumps(columns, indent=4)
    indented_json = "\n".join("                " + line for line in pretty_json.splitlines())

    template = Path(template_path).read_text(encoding="utf-8")
    result = template.replace("<schema>", indented_json)

    final_output = Path(f"{output_file}{pipe_line}_config.txt")
    final_output.write_text(result, encoding="utf-8")




def generate_json_standardization(
    file_path: str = 'dbx_context_param_optima.xlsx',
    sheet: str = 'schema',
    output_file: str = "",
    pipe_line: str = "",
    template_path: str = "",
    output_path: str = "",
    p_header: str = '',
    p_partition_column: str = ''
) -> None:

    if not template_path:
        raise ValueError("template_path must be provided")
    if not output_file:
        raise ValueError("output_file must be provided")

    df = pd.read_excel(file_path, sheet_name=sheet)
    df = df.iloc[:, 0:4]


    columns = []
    target_partition_list = []
    partitions = p_partition_column.split(", ")    

    # FOR TARGET PARTITION LIST
    for p in partitions:
        if p == "txn_date" or p == "txn_dt":
            target_partition_list.append({
                    "partition_name": "txn_date",
                    "data_type": "date",
                    "format": "%Y-%m-%d",
                    "metadata_table_column_ref": "transaction_timestamp",
                    "order": 1
                })
        elif p == "file_date" or p == "file_dt":
            target_partition_list.append({
                    "partition_name": "file_date",
                    "data_type": "date",
                    "format": "%Y-%m-%d",
                    "metadata_table_column_ref": "transaction_timestamp",
                    "order": 2
                })
        # elif p == "load_date":
        #     target_partition_list.append({
        #             "partition_name": "load_date",
        #             "data_type": "date",
        #             "format": "%Y-%m-%d",
        #             "metadata_table_column_ref": "transaction_timestamp",
        #             "order": 2
        #         })
        elif p == "file_timestamp_bucket":
            target_partition_list.append({
                    "partition_name": "file_timestamp_bucket",
                    "data_type": "date",
                    "format": "%Y-%m-%d%H",
                    "metadata_table_column_ref": "file_timestamp",
                    "order": 1
                })
        else:
            continue



    # FOR STANDARDIZATION
    if p_header == "true":
        for _, row in df.iterrows():
            iterator = str(row.iloc[0]).strip()
            field_name = str(row.iloc[1]).strip()
            data_type = str(row.iloc[2]).strip()
            field_name = field_name.lower()
            if not field_name or not data_type:
                continue

            if data_type.lower() == "timestamp" and field_name != "dbx_process_dttm":
                columns.append({
                    "standardize_function": "standardize_timestamp",
                    "source_column_name": field_name,
                    "additional_parameters": {
                        "timestamp_format": "yyyy-MM-dd HH:mm:ss",
                        "target_column_name": field_name
                    }
                })
            elif data_type.lower() == "timestamp" and field_name == "file_timestamp_bucket":
                columns.append({
                    "standardize_function": "standardize_date",
                    "additional_parameters": {
                        "date_format": "yyyy-MM-dd HH:mm:ss",
                        "target_column_name": field_name
                    }
                })
            elif data_type.lower() == "date" and field_name == "txn_dt":
                columns.append({
                    "standardize_function": "standardize_date",
                    "source_column_name": field_name,
                    "additional_parameters": {
                        "date_format": "yyyy-MM-dd",
                        "target_column_name": "txn_date"
                    }
                })
            # elif data_type.lower() == "date" and field_name == "load_date":
            #     columns.append({
            #         "standardize_function": "standardize_date",
            #         "additional_parameters": {
            #             "date_format": "yyyy-MM-dd",
            #             "target_column_name": "load_date"
            #         }
            #     })
            # elif data_type.lower() == "date" and field_name == "process_dt":
            #     columns.append({
            #         "standardize_function": "standardize_date",
            #         "source_column_name": field_name,
            #         "additional_parameters": {
            #             "date_format": "yyyy-MM-dd",
            #             "target_column_name": "process_date"
            #         }
            #     })
            elif data_type.lower() == "date" and field_name != "file_date":
                columns.append({
                    "standardize_function": "standardize_date",
                    "source_column_name": field_name,
                    "additional_parameters": {
                        "date_format": "yyyy-MM-dd",
                        "target_column_name": field_name
                    }
                })
            elif data_type.lower() == "date" and field_name == "file_date":
                columns.append({
                    "standardize_function": "standardize_date",
                    "additional_parameters": {
                        "date_format": "yyyy-MM-dd",
                        "target_column_name": field_name
                    }
                })
            elif field_name in {
                "owning_subscriber_id", "msisdn", "subscriber_id", "sub_id",
                "ret_min", "ret_msisdn", "dsp_min", "dealer_min"}:
                columns.append({
                    "standardize_function": "standardize_msisdn",
                    "source_column_name": field_name,
                    "additional_parameters": {
                        "target_column_name": field_name
                    }
                })
            else:
                continue
    else:
        for _, row in df.iterrows():
            iterator = str(row.iloc[0]).strip()
            field_name = str(row.iloc[1]).strip()
            data_type = str(row.iloc[2]).strip()

            if not field_name or not data_type:
                continue

            if data_type.lower() == "timestamp" and field_name != "dbx_process_dttm":
                columns.append({
                    "standardize_function": "standardize_timestamp",
                    "source_column_name": iterator,
                    "additional_parameters": {
                        "timestamp_format": "yyyy-MM-dd HH:mm:ss",
                        "target_column_name": field_name
                    }
                })
            elif data_type.lower() == "timestamp" and field_name == "file_timestamp_bucket":
                columns.append({
                    "standardize_function": "standardize_date",
                    "additional_parameters": {
                        "date_format": "yyyy-MM-dd HH:mm:ss",
                        "target_column_name": field_name
                    }
                })
            elif data_type.lower() == "date" and field_name != "file_date":
                columns.append({
                    "standardize_function": "standardize_date",
                    "source_column_name": iterator,
                    "additional_parameters": {
                        "date_format": "yyyy-MM-dd",
                        "target_column_name": field_name
                    }
                })
            elif data_type.lower() == "date" and field_name == "file_date":
                columns.append({
                    "standardize_function": "standardize_date",
                    "additional_parameters": {
                        "date_format": "yyyy-MM-dd",
                        "target_column_name": field_name
                    }
                })
            elif data_type.lower() == "date" and field_name == "txn_dt":
                columns.append({
                    "standardize_function": "standardize_date",
                    "source_column_name": field_name,
                    "additional_parameters": {
                        "date_format": "yyyy-MM-dd",
                        "target_column_name": "txn_date"
                    }
                })
            # elif data_type.lower() == "date" and field_name == "load_date":
            #     columns.append({
            #         "standardize_function": "standardize_date",
            #         "additional_parameters": {
            #             "date_format": "yyyy-MM-dd",
            #             "target_column_name": "load_date"
            #         }
            #     })
            # elif data_type.lower() == "date" and field_name == "process_dt":
            #     columns.append({
            #         "standardize_function": "standardize_date",
            #         "source_column_name": field_name,
            #         "additional_parameters": {
            #             "date_format": "yyyy-MM-dd",
            #             "target_column_name": "process_date"
            #         }
            #     })
            elif field_name in {
                "owning_subscriber_id", "msisdn", "subscriber_id", "sub_id",
                "ret_min", "ret_msisdn", "dsp_min", "dealer_min"
            }:
                columns.append({
                    "standardize_function": "standardize_msisdn",
                    "source_column_name": iterator,
                    "additional_parameters": {
                        "target_column_name": field_name
                    }
                })
            else:
                continue

    pretty_json = json.dumps(columns, indent=4)
    indented_json = "\n".join("           " + line for line in pretty_json.splitlines())

    partition_pretty_json = json.dumps(target_partition_list, indent=4)
    partition_indented_json = "\n".join("           " + line for line in partition_pretty_json.splitlines())

    template = Path(template_path).read_text(encoding="utf-8")
    result = template.replace("<standardization>", indented_json)
    result = result.replace("<target_partition_list>", partition_indented_json)
    final_output = Path(f"{output_path}")
    final_output.write_text(result, encoding="utf-8")

    print(f"✅ JSON CONFIG {final_output.resolve()}")




def replace_templated_word_for_json_config(
    input_path: Path,
    output_path: Path,
    replacements: dict[str, str],
    file_path: str = 'dbx_context_param_optima.xlsx',
    sheet: str = 'schema') -> None:

    
    df = pd.read_excel(file_path, sheet_name=sheet)
    row_count = len(df)
    row_count_str = str(row_count)
    
    try:
        content = input_path.read_text()
        for target, replacement in replacements.items():
            content = content.replace(target, replacement)
            content = content.replace("<column_count>", row_count_str)
        output_path.write_text(content)
    
    except FileNotFoundError:
        print(f"❌ Template file not found: {input_path}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def remove_trash(trash_path: Path) -> None:
    for file in trash_path.glob("*.*"):
        if file.is_file():
            file.unlink()
            print(f"🗑️  Clean-up trash: {file}")
    


def read_excel_file(file_path: str = 'dbx_context_param_optima.xlsx', sheet: str = 'newcontext') -> ContextParam:
    df = pd.read_excel(file_path, sheet_name=sheet, usecols="C", skiprows=1, nrows=23)

    if df.empty:
        raise ValueError("Excel range is empty.")

    values = df.iloc[:, 0].tolist()
    valid_keys = [f.name for f in fields(ContextParam)]
    print(valid_keys)
    filtered_data = dict(zip(valid_keys, values))

    return ContextParam(**filtered_data)



def dbx_main():

    load_dotenv()
    BASE_PATH = os.getenv('BASE_PATH')
    context = read_excel_file(file_path=rf'{BASE_PATH}dbx_context_param_optima.xlsx', sheet='newcontext')

    parent_output_path = rf"{BASE_PATH}output\\"
    parent_template_path = rf"{BASE_PATH}templates\\phase1\\"
    parent_trash_path = rf"{BASE_PATH}trash\\"

    config = get_cluster_values(context.p_file_size, context.p_soure_file_format)
    node_type_id = config["node_type_id"]
    first_on_demand = config["first_on_demand"]
    num_workers = config["num_workers"]
    min_workers = config["min_workers"]
    max_workers = config["max_workers"]


    if context.p_header:
        p_header = "true"
    else:
        p_header = "false"

    if isinstance(context.p_delimeter, float) and math.isnan(context.p_delimeter):
        context.p_delimeter = ""

    if isinstance(context.tier_suffix, float) and math.isnan(context.tier_suffix):
        context.tier_suffix = ""


    data_refresh_column_list = context.p_partition_column
    r_data_refresh_column_list = ", ".join(f'"{p.strip()}"' for p in data_refresh_column_list.split(","))

    string_to_replace = {
        "<work_space>": context.p_work_space,
        "<pipeline>": context.p_pipeline,
        "<frequency>": context.p_frequency,
        "<p_tier>" : context.p_tier,
        "<tier>": context.p_tier,
        "<data_domain>": context.p_data_domain,
        "<file_mask>": context.on_prem_p_file_mask,
        "<tier_suffix>": context.tier_suffix,
        "<table_name>": context.p_table_name,
        "<delimeter>": context.p_delimeter,
        "<soure_file_format>": context.p_soure_file_format,
        "<header_flag>": p_header,
        "<save_mode>": context.p_save_mode,
        "<source_directory>": context.p_source_directory,
        "<application_name>": context.p_application_name,
        "<p_task_key_name>": context.p_task_key_name,
        "<p_partition_column>": context.p_partition_column,
        "<p_retention_key>": context.p_retention_key,
        "<data_refresh_column_list>": r_data_refresh_column_list,
        "<node_type_id>": str(node_type_id),
        "<first_on_demand>": str(first_on_demand),
        "<num_workers>": str(num_workers),
        "<min_workers>": str(min_workers),
        "<max_workers>": str(max_workers),
        "<job_yml_file>": f"{context.p_pipeline}_job.yml",
        "<alter_tags_file>": f"alter_tags-{context.p_pipeline}.sql",
        "<json_file>": f"{context.p_pipeline}_config.json",
        "<sql_file>": f"{context.p_pipeline}.sql",
        "<s3_source_file_dir>": context.s3_source_file_dir,
        "<s3_source_file_mask>": context.s3_source_file_mask,
        "<s3_manifest_file_mask>": context.s3_manifest_file_mask            

    }

    string_to_replace = {
        k: "" if v is None or (isinstance(v, float) and math.isnan(v)) else str(v)
        for k, v in string_to_replace.items()
    }
    
    generate_job_yml(
        input_path=Path(rf"{parent_template_path}dbx_job_template_optima.txt"),
        output_path=Path(rf"{parent_output_path}{context.p_pipeline}_job.yml"),
        replacements=string_to_replace
    )


    # dev_generate_json_config_data_sync(
    #     input_path=Path(rf"{parent_template_path}dev_s3_bucket_config_template.txt"),
    #     output_path=Path(rf"{parent_output_path}dev_gdm_config-{context.p_pipeline}.json"),
    #     replacements=string_to_replace
    # )

    # pet_generate_json_config_data_sync(
    #     input_path=Path(rf"{parent_template_path}pet_s3_pet_json_config_template.txt"),
    #     output_path=Path(rf"{parent_output_path}pet_gdm_config-{context.p_pipeline}.json"),
    #     replacements=string_to_replace
    # )

    # data_sync_runner(
    #     input_path=Path(rf"{parent_template_path}data_sync_runner.txt"),
    #     output_path=Path(rf"{parent_output_path}job_runner_{context.p_pipeline}.sh"),
    #     replacements=string_to_replace
    # )

    generate_grants(
        input_path=Path(rf"{parent_template_path}grants_dev_pet_prod.txt"),
        output_path=Path(rf"{parent_output_path}grants_onboarding-{context.p_pipeline}.sql"),
        replacements=string_to_replace
    )

    generate_sql(
        table_name = context.p_table_name, 
        output_file = parent_output_path, 
        p_pipeline = context.p_pipeline, 
        p_tier=context.p_tier,
        p_header=p_header
    )

    generate_onboarding_ddl(
        table_name=context.p_table_name, 
        work_space=context.p_work_space, 
        data_domain=context.p_data_domain,
        tier_suffix=context.tier_suffix,
        p_partition_column=context.p_partition_column,
        p_retention_key=context.p_retention_key,
        output_file=parent_output_path
    )

    generate_table_tags(
        table_name = context.p_table_name,
        output_file = Path(rf"{parent_output_path}alter_tags-{context.p_pipeline}.sql"),
        work_space = context.p_work_space,
        data_domain = context.p_data_domain,
        tier_suffix = context.tier_suffix
    )

    generate_json_config(
        output_file=parent_trash_path, 
        pipe_line=context.p_pipeline, 
        template_path=f"{parent_template_path}dbx_json_template_optima.txt"
    )


    json_config_p = f"{parent_output_path}{context.p_pipeline}_config.json"


    replace_templated_word_for_json_config(
        input_path=Path(f"{parent_trash_path}{context.p_pipeline}_config.txt"),
        replacements=string_to_replace, 
        output_path=Path(f"{parent_trash_path}{context.p_pipeline}_config.txt")
    )

    generate_json_standardization(
        output_file=parent_trash_path, 
        pipe_line=context.p_pipeline, 
        template_path=f"{parent_trash_path}{context.p_pipeline}_config.txt",
        output_path=Path(f"{json_config_p}"),
        p_header=p_header,
        p_partition_column=context.p_partition_column
    )

    # generate_instructions(
    #     input_path=Path(rf"{parent_template_path}instructions.txt"),
    #     output_path=Path(rf"{parent_output_path}instructions.txt"),
    #     replacements=string_to_replace
    # )


    remove_trash(trash_path=Path(parent_trash_path))


if __name__ == "__main__":
    dbx_main()
