#!/bin/bash

# ================================================================
#  Data Movement Script
#  Author: <Your Name>
#  Purpose: Generate config JSON and run S3 ingestion job
#  Usage:
#    ./data_move.sh <process_mode> <environment>
#      - process_mode : e.g. "B" or "R"
#      - environment  : non_prod | pet | prod
# ================================================================

echo "$(date) - Application Started"
curr_dt=$(date +"%Y%m%d")
process_mode=${1:-"B"}
environment=${2:-"non_prod"}   # Default = non_prod

# -----------------------------
# Environment Configurations
# -----------------------------
case "${environment}" in
  non_prod)
    lz_bucket_name="pldt-smart-nonprod-120569600034-ap-southeast-1-gdm-wde-landing"
    manifest_bucket_name="pldt-smart-nonprod-120569600034-ap-southeast-1-gdm-wde-monitor"
    vpc_endpoint="https://bucket.vpce-0cb360879a11cdca5-vg6052s8.s3.ap-southeast-1.vpce.amazonaws.com"
    ;;
  pet)
    lz_bucket_name="pldt-smart-pet-412381746361-ap-southeast-1-gdm-wde-landing"
    manifest_bucket_name="pldt-smart-pet-412381746361-ap-southeast-1-gdm-wde-monitor"
    vpc_endpoint="https://bucket.vpce-09c0a6c0d7cfda590-sjolgv7k.s3.ap-southeast-1.vpce.amazonaws.com"
    ;;
  prod)
    lz_bucket_name="secret-prod"
    manifest_bucket_name="secret-prod"
    vpc_endpoint="secret-prod"
    ;;
  *)
    echo "❌ Invalid environment: ${environment}"
    echo "Valid values: non_prod | pet | prod"
    exit 1
    ;;
esac


config_dir="/home/apps_user/gdm_assets/${environment}/config"
config_filename="gdm_config-cox_nrt_acounthistory.json"
json_file="${config_dir}/${config_filename}"
workspace="wde"

# -----------------------------
# Generate JSON Config File
# -----------------------------
generate_json() {
  mkdir -p "${config_dir}"

  cat > "${json_file}" <<EOF
{
  "source_name": "cox_nrt_acounthistory",
  "source_directory": "/datahub/talend_prod/data/hive/gdm_landing/cox_nrt_acounthistory/",
  "valid_filename_prefixes": ["cox*"],
  "excluded_extensions": ["ipynb_checkpoints", "tmp", "temp", "_TEMPORARY"],
  "retrieve_file_line_count": "Y",
  "max_files_to_process": 50,
  "file_mtime_threshold": -1,
  "target_s3_bucket_name": "${lz_bucket_name}",
  "target_s3_landing_directory": "cox_nrt_acounthistory/raw/",
  "target_aws_region": "ap-southeast-1",
  "source_manifest_s3_bucket_name": "${manifest_bucket_name}",
  "source_manifest_s3_landing_directory": "manifests/source_manifest/cox_nrt_acounthistory/raw/",
  "housekeeping_manifest_s3_bucket_name": "${manifest_bucket_name}",
  "housekeeping_manifest_s3_landing_directory": "manifests/housekeeping_manifest/cox_nrt_acounthistory/raw/",
  "job_log_s3_bucket_name": "${manifest_bucket_name}",
  "job_log_s3_landing_directory": "logs/job_logs/cox_nrt_acounthistory/",
  "s3_vpc_endpoint_url": "${vpc_endpoint}",
  "databricks_workspace_name": "${workspace}",
  "databricks_environment": "${environment}"
}
EOF
}

# -----------------------------
# Execute Data Movement Job
# -----------------------------
run_job() {
  echo "Running data movement job for [${environment}]..."
  /scripts/execute_ops_nonprod.sh -s "${config_filename}" -p "${process_mode}"
}

# -----------------------------
# Main
# -----------------------------
generate_json
run_job

echo "$(date) - Application Ended"
