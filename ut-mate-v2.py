%pip install -U mlflow databricks-sdk[openai]
%restart_python

import glob
import json
from typing import Dict, List, Any, Union, Set, Optional, Tuple
from dataclasses import dataclass, field
import os
import pandas as pd
import re
import yaml
import mlflow
from databricks.sdk import WorkspaceClient
from pathlib import Path

mlflow.openai.autolog()
mlflow.set_experiment(experiment_id=2347514886748068)


@dataclass
class KeyInfo:
    path: str
    key: str
    type_name: str
    value: Any = None

@dataclass
class JSONKeyExtractor:
    keys: Set[str] = field(default_factory=set)
    paths: List[str] = field(default_factory=list)
    key_types: Dict[str, str] = field(default_factory=dict)
    key_value_pairs: Dict[str, Any] = field(default_factory=dict)  # path -> value

    def extract_keys(self, data: Union[str, Dict, List], current_path: str = "") -> None:
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON: {e}")
        self._extract_recursive(data, current_path)

    def _extract_recursive(self, obj: Any, current_path: str = "") -> None:
        if obj is None:
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{current_path}.{key}" if current_path else key
                self.keys.add(key)
                self.paths.append(new_path)
                self.key_types[new_path] = type(value).__name__
                self.key_value_pairs[new_path] = value
                if isinstance(value, (dict, list)):
                    self._extract_recursive(value, new_path)
        elif isinstance(obj, list):
            for index, item in enumerate(obj):
                new_path = f"{current_path}[{index}]" if current_path else f"[{index}]"
                if isinstance(item, (dict, list)):
                    self._extract_recursive(item, new_path)

    def get_keys(self, 
                 include_nested: bool = True,
                 show_path: bool = False,
                 unique_only: bool = True,
                 sort_keys: bool = False,
                 filter_by_type: Optional[str] = None) -> List[str]:
        if show_path:
            result = self.paths.copy()
            if filter_by_type:
                result = [path for path in result 
                          if self.key_types.get(path, '').lower() == filter_by_type.lower()]
        else:
            if unique_only:
                result = list(self.keys)
            else:
                result = []
                for path in self.paths:
                    key = path.split('.')[-1].split('[')[0]
                    result.append(key)
        if sort_keys:
            result.sort()
        return result

    def get_pairs(self, 
                  show_path: bool = True,
                  filter_by_type: Optional[str] = None,
                  sort_keys: bool = False) -> List[Tuple[str, Any]]:
        pairs = []
        for path in self.paths:
            if filter_by_type and self.key_types.get(path, '').lower() != filter_by_type.lower():
                continue
            key = path if show_path else path.split('.')[-1].split('[')[0]
            value = self.key_value_pairs.get(path)
            pairs.append((key, value))
        if sort_keys:
            pairs.sort(key=lambda x: x[0])
        return pairs

    def get_statistics(self) -> Dict[str, Any]:
        type_counts = {}
        for key_type in self.key_types.values():
            type_counts[key_type] = type_counts.get(key_type, 0) + 1
        return {
            'total_keys': len(self.paths),
            'unique_keys': len(self.keys),
            'max_depth': max([path.count('.') for path in self.paths] + [0]),
            'type_distribution': type_counts
        }

    def reset(self) -> None:
        self.keys.clear()
        self.paths.clear()
        self.key_types.clear()
        self.key_value_pairs.clear()

    def export_results(self, 
                      format_type: str = 'list',
                      include_types: bool = False,
                      include_pairs: bool = False,
                      **kwargs) -> Union[List[str], Dict[str, Any], str]:
        if include_pairs:
            pairs = self.get_pairs(
                show_path=kwargs.get('show_path', True),
                filter_by_type=kwargs.get('filter_by_type', None),
                sort_keys=kwargs.get('sort_keys', False)
            )
            if format_type == 'list':
                return [f"{k}: {v}" for k, v in pairs]
            elif format_type == 'dict':
                return {k: v for k, v in pairs}
            elif format_type == 'json':
                return json.dumps({k: v for k, v in pairs}, indent=2)
            elif format_type == 'csv':
                lines = ['key,value']
                for k, v in pairs:
                    lines.append(f'"{k}","{v}"')
                return '\n'.join(lines)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
        else:
            keys = self.get_keys(**kwargs)
            if format_type == 'list':
                return keys
            elif format_type == 'dict':
                if include_types:
                    return {path: self.key_types.get(path, 'unknown') 
                            for path in self.paths}
                else:
                    return {i: key for i, key in enumerate(keys)}
            elif format_type == 'json':
                if include_types:
                    data = {path: self.key_types.get(path, 'unknown') 
                            for path in self.paths}
                else:
                    data = keys
                return json.dumps(data, indent=2)
            elif format_type == 'csv':
                if include_types:
                    lines = ['key,type']
                    for path in self.paths:
                        key_type = self.key_types.get(path, 'unknown')
                        lines.append(f'"{path}","{key_type}"')
                else:
                    lines = ['key'] + [f'"{key}"' for key in keys]
                return '\n'.join(lines)
            else:
                raise ValueError(f"Unsupported format: {format_type}")

    def get_values_by_key(self, search_key: str) -> List[Any]:
        values = []
        for path in self.paths:
            key = path.split('.')[-1].split('[')[0]
            if key == search_key:
                values.append(self.key_value_pairs[path])
        return values

    def key_exists(self, search_key: str) -> bool:
        return search_key in self.keys

    def get_key_value_pairs(self, search_key: str) -> List[Tuple[str, Any]]:
        pairs = []
        for path in self.paths:
            key = path.split('.')[-1].split('[')[0]
            if key == search_key:
                pairs.append((path, self.key_value_pairs[path]))
        return pairs

    def get_subchild_list_items(self, search_key: str) -> Dict[str, List[Any]]:
        subchild_items = {}
        for path in self.paths:
            key = path.split('.')[-1].split('[')[0]
            if key == search_key and isinstance(self.key_value_pairs[path], list):
                subchild_items[path] = self.key_value_pairs[path]
        return subchild_items

def print_statistics(stats: Dict[str, Any]):
    print("\n=== Statistics ===")
    print(f"Total keys: {stats['total_keys']}")
    print(f"Unique keys: {stats['unique_keys']}")
    print(f"Max depth: {stats['max_depth']}")
    print("\nType distribution:")
    for type_name, count in sorted(stats['type_distribution'].items()):
        print(f"  {type_name}: {count}")
    print()

def main_extractor(input_path, key, feature):
    input_path = input_path
    search_key = key
    feature = feature
    output_path = "output_keys.txt"
    output_format = "list"
    show_path = False
    include_types = False
    sort_keys = False
    unique_only = True
    filter_by_type = None
    show_stats = True
    include_pairs = True 

    if not os.path.exists(input_path):
        print(f"Error: File not found: {input_path}")
        return
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            json_data = f.read()
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    extractor = JSONKeyExtractor()
    try:
        extractor.extract_keys(json_data)
    except ValueError as e:
        print(f"Error: {e}")
        return

    if not extractor.paths:
        print("No keys found in the JSON data")
        return

    try:
        results = extractor.export_results(
            format_type=output_format,
            show_path=show_path,
            include_types=include_types,
            sort_keys=sort_keys,
            unique_only=unique_only,
            filter_by_type=filter_by_type,
            include_pairs=include_pairs
        )
    except ValueError as e:
        print(f"Error: {e}")
        return

    # output_text = ""
    # if isinstance(results, list):
    #     output_text = '\n'.join(results)
    # else:
    #     output_text = str(results)

    # try:
    #     with open(output_path, 'w', encoding='utf-8') as f:
    #         f.write(output_text)
    #     print(f"Results saved to {output_path}")
    # except Exception as e:
    #     print(f"Error writing to file: {e}")
    #     return

    # if show_stats:
    #     stats = extractor.get_statistics()
    #     print_statistics(stats)

    if feature == "1":
        exists = extractor.key_exists(search_key)
        if exists:
            # print(f"✅ Key '{search_key}' exists in the JSON.")
            return search_key
        else:
            print(f"❌ Key '{search_key}' does NOT exist in the JSON.")
    elif feature == "2":
        values = extractor.get_values_by_key(search_key)
        if values:
            for idx, value in enumerate(values):
                # print(f"  [{idx}]: {value}")
                return value
        else:
            print(f"No values found for key '{search_key}'.")
    elif feature == "3":
        subchilds = extractor.get_subchild_list_items(search_key)
        if not subchilds:
            # Try to find subchilds by matching full path if search_key is a path
            for path in extractor.paths:
                if search_key in path and isinstance(extractor.key_value_pairs[path], list):
                    subchilds[path] = extractor.key_value_pairs[path]
        for path, items in subchilds.items():
            for idx, item in enumerate(items):
                # print(f"    [{idx}]: {item}")
                return item



def extract_schema_from_check_schema_consistency(data_quality_configuration):
    schema_dict = {}
    for rule in data_quality_configuration:
        if rule.get("rule_name") == "check_schema_consistency":
            for col in rule.get("parameters", {}).get("expected_schema", []):
                column_name = col.get("column_name")
                data_type = col.get("data_type")
                if column_name and data_type:
                    schema_dict[column_name] = data_type
    return schema_dict

def test_schema_matches(schema_dict, schema_kv):
    results = []
    for col, expected_type in schema_dict.items():
        actual_type = schema_kv.get(col)
        if actual_type is None:
            if col == "file_id":
                continue
            results.append({
                "status": "❌",
                "config_column": col,
                "config_datatype": expected_type,
                "table_column": None,
                "table_datatype": None
            })
        elif actual_type.lower() == expected_type.lower():
            results.append({
                "status": "✅",
                "config_column": col,
                "config_datatype": expected_type,
                "table_column": col,
                "table_datatype": actual_type
            })
        else:
            results.append({
                "status": "❌",
                "config_column": col,
                "config_datatype": expected_type,
                "table_column": col,
                "table_datatype": actual_type
            })
    for col in schema_kv:
        if col not in schema_dict:
            results.append({
                "status": "⚠️",
                "config_column": None,
                "config_datatype": None,
                "table_column": col,
                "table_datatype": schema_kv[col]
            })
    display(pd.DataFrame(results))
    # df = pd.DataFrame(results)
    # display(df)
    # for _, row in df.iterrows():
    #     # log_and_report(job, "Schema consistency", f"{row.to_dict()}")
    #     log_and_report(job, "Schema consistency", f"{row}")
    all(r["status"] == "✅" or r["status"] == "⚠️" for r in results), "Schema mismatch found between expected and actual table schema"

    print()
    # print(f"{'-'*55} CHECKING OF FIELD TOTAL COUNT FROM CONFIG VS CATALOG TABLE {'-'*55}")
    schema_dict_cnt = sum(1 for col in schema_dict)
    schema_kv_cnt = sum(1 for col in schema_kv)
    if schema_dict_cnt != schema_kv_cnt +1:
        print(f"❌ Column count mismatch: config={schema_dict_cnt}, table={schema_kv_cnt}")
        print(f"❌ It should not be equal, the total count from config should be +1 vs the total count from table because of file_id")
    else:
        print(f"✅ Config Json schema column count is : {schema_dict_cnt} because of file_id")
        print(f"✅ Catalog Table schema column count: {schema_kv_cnt} because we dont include file_id in table")
    print()



def extract_partition_columns_from_create_table(dev_tbl_df):
    create_table_stmt = spark.sql(f"show create table {dev_tbl_df}").collect()[0][0]
    partitioned_by_match = re.search(r"PARTITIONED BY\s*\((.*?)\)", create_table_stmt, re.DOTALL)
    if partitioned_by_match:
        partition_cols = [col.strip().split()[0] for col in partitioned_by_match.group(1).split(",")]
        # print(partition_cols)
        return partition_cols
    else:
        print("No PARTITIONED BY clause found.")
        return []

# display(spark.sql("show create table wde_dev.cu_b.opt_dly_descriptions"))


def rules_for_parquet(config, file_format):
    allowed_rules = {
        "compare_file_size_in_bytes",
        "reconcile_file_set",
        "check_schema_consistency",
        "check_column_count",
        "check_null",
    }
    rule_names = [rule.get("rule_name") for rule in config]
    if file_format.lower() == "parquet":
        invalid_rules = [r for r in rule_names if r not in allowed_rules]
        if invalid_rules:
            print(f"❌ Invalid rules for parquet: {invalid_rules}")
        else:
            print(f"✅ All rules are valid for {file_format}: {rule_names}")
    else:
        print(rule_names)
        print(file_format)

def rules_for_delimited_file(config, file_format, delimiter):

    if file_format.lower() in ["csv", "tsv", "txt"]:
        if not delimiter:
            print(f"❌ file_format '{file_format}' should have a non-empty delimiter.")
        else:
            print(f"✅ Delimiter for file_format '{file_format}': '{delimiter}'")
    allowed_rules = {
        "compare_file_size_in_bytes",
        "reconcile_file_set",
        "check_schema_consistency",
        "check_column_count",
        "check_null",
        "compare_row_count"
    }
    rule_names = [rule.get("rule_name") for rule in config]
    if file_format.lower() in ["csv", "tsv", "txt"]:
        invalid_rules = [r for r in rule_names if r not in allowed_rules]
        if invalid_rules:
            print(f"❌ Invalid rules for parquet: {invalid_rules}")
        else:
            print(f"✅ All rules are valid for {file_format} : {rule_names}")
    else:
        print(rule_names)
        print(file_format)



def read_file(file_path):
    return Path(file_path).read_text()

def generate_output(client, model, user_content, choice_idx=0):
    system_content_template = (
        "You are an AI automation assistant specialized in generating helpful, human-readable "
        "descriptions and comments for database columns or schema fields you can base under catalog. "
        "Your primary goal is to provide concise, clear, and contextually accurate suggestions short description"
        "that explain the purpose or meaning of a column, making it easier for users and developers "
        "to understand the data. "
        "Descriptions should be professional, unambiguous, and aligned with best practices for "
        "data documentation. Avoid redundancy and focus on clarity. "
        "If the column name is technical, expand it into plain language; "
        "if its ambiguous, infer meaning from context. "
        "Always use consistent tone and style across descriptions. "
        "The goal is to minimize human guesswork and improve overall data understanding."
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_content_template},
            {"role": "user", "content": user_content},
        ]
    )
    return response.choices[choice_idx].message.content




def check_table_column_comments(table_name="", tags_file_path="GDM_Databricks_Tag_List.csv", model_to_use=""):

    tags_file_path = tags_file_path

    df_spark = spark.sql(f"DESCRIBE EXTENDED {table_name}")
    properties_not_to_be_include = ['Catalog','Database','Table','Created Time','Last Access','Created By','Type','Location','Provider','Owner','Is_managed_location','Predictive Optimization','Table Properties','# col_name', 'Column Names', 'Column Selection Method', 'Statistics']
    columns_section = df_spark.where("col_name != '' and data_type != ''")
    columns = columns_section.select("col_name", "comment").collect()
    results = []

    df_csv = pd.read_csv(tags_file_path)

    for _, tag_row in df_csv.iterrows():
        tag_content = tag_row.to_dict()
        field_names = str(tag_content.get('field_name'))
        tag_value = str(tag_content.get('tag_value'))
        tag_key = str(tag_content.get('tag_key'))
        tag_status = ""
        tag_command = ""
        for row in columns:
            col_name = row["col_name"]
            comment = row["comment"]

            if col_name in properties_not_to_be_include:
                continue
            if any(r["column"] == col_name for r in results):
                continue

            status = ""
            if comment and str(comment).strip() :
                status = "✅"

                if col_name == field_names:
                    tag_status = "✅"
                    tag_command = f"ALTER TABLE {table_name} ALTER COLUMN {col_name} SET TAGS ('{tag_key}' = '{tag_value}');"
                elif col_name != field_names:
                    tag_status = ""
                    tag_command = ""
                else:    
                    tag_status = ""

            else:
                status = "❌"


            if  status == "❌":
                user_content = f""" give a suggestion for this column/field {col_name} make it and prefessional that follows data governance you can base on the catalog, lastly the result should be one line sentence give me only the suggested description because I dont have to see the other text"""
                suggested_comment = generate_output(
                    client,
                    model_to_use,
                    user_content,
                    choice_idx=0
                )
            else:
                suggested_comment = ""

            results.append({
                "column": col_name,
                "has_comment": status,
                "comment": comment,
                "suggested_comment": suggested_comment,
                "tags_status": tag_status,
                "tags_command": tag_command
            })

    display(pd.DataFrame(results))



if __name__ == "__main__":
    w = WorkspaceClient()
    client = w.serving_endpoints.get_open_ai_client()

    yml_path = ".bundle/root/local/files/databricks.yml"
    etc_path = ".bundle/root/local/files/src/pldt/cu/etc/"
    tags_file_path = "GDM_Databricks_Tag_List.csv"




    with open(yml_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    active_jobs = [line.strip() for line in lines if line.lstrip().startswith("- ./") and not line.lstrip().startswith("# - ./")]
    filtered_jobs = []
    for job in active_jobs:
        if job.startswith("- ./") and job.endswith(".yml"):
            job_name = job.split("/")[-1].replace(".yml", "")
            job_name = job_name.replace("_job", "")
            filtered_jobs.append(job_name)

    for job in filtered_jobs:
        for file_path in glob.glob(f"{etc_path}*{job}*"):

                print("#"*80)
                print(job.center(80))
                print("#"*80)

                task_configuration = main_extractor(input_path=file_path, key="task_configuration", feature="2")
                file_format = main_extractor(input_path=file_path, key="file_format", feature="2")
                delimiter = main_extractor(input_path=file_path, key="delimiter", feature="2")
                data_quality_configuration = main_extractor(input_path=file_path, key="data_quality_configuration", feature="2")
                table_name = main_extractor(input_path=file_path, key="table_name", feature="2")
                target_partition_list = main_extractor(input_path=file_path, key="target_partition_list", feature="2")
                target_partition_list = [partition["partition_name"] for partition in target_partition_list] if isinstance(target_partition_list, list) else []

                schema_dict = extract_schema_from_check_schema_consistency(data_quality_configuration)
                table_name_parts = table_name.split(".")
                wde_index = next(i for i, part in enumerate(table_name_parts) if part.startswith("wde"))
                table_name_parts[wde_index] = table_name_parts[wde_index] + "_dev"
                dev_table_name = ".".join(table_name_parts)

                dev_tbl_df = spark.table(dev_table_name)
                schema_kv = {field.name: field.dataType.simpleString() for field in dev_tbl_df.schema.fields}
                test_schema_matches(schema_dict, schema_kv)

                # print(f"-"*60, "CHECKING OF CONFIG PARTITION VS CATALOG TABLE PARTITION", "-"*60)
                print(f"")
                table_partition = extract_partition_columns_from_create_table(dev_table_name)
                for partition in target_partition_list:
                    if partition in table_partition:
                        print(f"✅ Partition or Column '{partition}' found in table {dev_table_name}")
                    else:
                        print(f"❌ Partition or Column '{partition}' NOT found in table {dev_table_name}")

                # print(f"-"*50, "CHECKING OF CONFIG RULES NEED TO APPLY DEPENDING ON THE FILE FORMAT", "-"*50)
                # print(f"")
                if file_format.lower() == "parquet":
                    rules_for_parquet(data_quality_configuration, file_format)
                elif file_format.lower() == "csv": 
                    rules_for_delimited_file(data_quality_configuration, file_format, delimiter)
                else:
                    print()


                model_to_use = "databricks-llama-4-maverick"
                check_table_column_comments(table_name=dev_table_name, tags_file_path=tags_file_path, model_to_use=model_to_use)