import re

# Cluster specs mapping
WAREHOUSE_SIZES = {
    "2X-Small": {"node_type_id": "i3.xlarge", "worker_count": 1},
    "X-Small":   {"node_type_id": "i3.2xlarge", "worker_count": 2},
    "Small":     {"node_type_id": "i3.4xlarge", "worker_count": 4},
    "Medium":    {"node_type_id": "i3.8xlarge", "worker_count": 8},
    "Large":     {"node_type_id": "i3.8xlarge", "worker_count": 16},
    "X-Large":   {"node_type_id": "i3.16xlarge", "worker_count": 32},
    "2X-Large":  {"node_type_id": "i3.16xlarge", "worker_count": 64},
    "3X-Large":  {"node_type_id": "i3.16xlarge", "worker_count": 128},
    "4X-Large":  {"node_type_id": "i3.16xlarge", "worker_count": 256},
}

# def parse_file_size(size_str: str) -> int:
#     """Extract size in GB from a string like '2gb'."""
#     match = re.match(r"(\d+)", size_str.lower())
#     return int(match.group(1)) if match else 0

def parse_file_size(size_str: str) -> int:
    """Extract size in GB from a string like '2gb', '10GB', '500mb'. 
    Returns size in GB as int (rounded down if MB/KB given).
    """
    if not size_str or not isinstance(size_str, str):
        return 0

    size_str = size_str.strip().lower()

    # Match number + optional unit
    match = re.match(r"(\d+)\s*([a-z]*)", size_str)
    if not match:
        return 0

    value, unit = match.groups()
    value = int(value)

    # Normalize units
    if unit in ("gb", "g", ""):
        return value
    elif unit in ("mb", "m"):
        return value // 1024   # convert MB → GB
    elif unit in ("kb", "k"):
        return value // (1024 * 1024)   # convert KB → GB
    else:
        return value   # default: assume GB


def pick_warehouse_size(file_size_gb: int) -> str:
    """Granular mapping of file size to warehouse size."""
    if file_size_gb <= 1:
        return "2X-Small"
    elif file_size_gb <= 3:
        return "X-Small"
    elif file_size_gb <= 5:
        return "Small"
    elif file_size_gb <= 10:
        return "Medium"
    elif file_size_gb <= 20:
        return "Large"
    elif file_size_gb <= 50:
        return "X-Large"
    elif file_size_gb <= 100:
        return "2X-Large"
    elif file_size_gb <= 200:
        return "3X-Large"
    else:
        return "4X-Large"

def get_cluster_values(file_size: str ="4gb", file_format: str = "csv") -> dict:
    size_gb = parse_file_size(file_size)
    warehouse_size = pick_warehouse_size(size_gb)
    specs = WAREHOUSE_SIZES[warehouse_size]

    node_type_id = specs["node_type_id"]
    num_workers = specs["worker_count"]
    min_workers = max(1, num_workers // 2)
    max_workers = num_workers * 2
    first_on_demand = num_workers

    return {
        "warehouse_size": warehouse_size,
        "node_type_id": node_type_id,
        "first_on_demand": first_on_demand,
        "num_workers": num_workers,
        "min_workers": min_workers,
        "max_workers": max_workers
    }

if __name__ == "__main__":
    file_size = ""
    file_format = ""

    config = get_cluster_values(file_size, file_format)
    print(f"{file_size} ({file_format}): {config}")
