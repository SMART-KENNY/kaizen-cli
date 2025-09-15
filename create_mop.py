import os
import pandas as pd
from components.c_generate_mop  import replace_placeholders_in_excel
from dotenv import load_dotenv
from dataclasses import dataclass, fields
from typing import Optional


@dataclass
class ContextParam:
    p_project_name: Optional[str] = None
    p_jobs_to_deploy: Optional[str] = None
    p_dev: Optional[str] = None
    p_implementation_date: Optional[str] = None


def read_excel_file(file_path: str = 'dbx_context_param.xlsx', sheet: str = 'newcontext') -> ContextParam:
    df = pd.read_excel(file_path, sheet_name=sheet, usecols="G", skiprows=6, nrows=15)

    if df.empty:
        raise ValueError("Excel range is empty.")

    values = df.iloc[:, 0].tolist()
    valid_keys = [f.name for f in fields(ContextParam)]
    filtered_data = dict(zip(valid_keys, values))

    return ContextParam(**filtered_data)



def create_mop():
    load_dotenv()
    BASE_PATH = os.getenv('BASE_PATH')

    parent_output_path = rf"{BASE_PATH}output\\"
    parent_template_path = rf"{BASE_PATH}templates\\phase1\\"

    context = read_excel_file(file_path=rf'{BASE_PATH}dbx_context_param.xlsx', sheet = 'newcontext')
    # print(context.p_project_name)
    # print(context.p_jobs_to_deploy)
    # print(context.p_dev)
    # print(context.p_implementation_date)

    replacements = {
        "<project_name>": context.p_project_name,
        "<jobs_to_deploy>": "        "+context.p_jobs_to_deploy,
        "<dev>": context.p_dev
    }

    output_file = replace_placeholders_in_excel(
        rf"{parent_template_path}\Alapaap_wde_mop_template.xlsx", 
        replacements, 
        rf"{parent_output_path}\{context.p_project_name}-mop.xlsx"
    )
    print(f"✅ MOP Created: {output_file}")

if __name__ == "__main__":
    create_mop()

