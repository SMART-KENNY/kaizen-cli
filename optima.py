from components.c_data_tech_cli import data_tech_cli
from components.c_user_option import ai_options
from components.c_dbx_generator_optima import dbx_main
from create_mop import create_mop
from components.c_create_sharepoint import open_link_safe
from components.c_execution_procedure import open_file_maximized
from dotenv import load_dotenv
import os

# data_tech_cli()
user_choice = ai_options()
load_dotenv()
BASE_PATH = os.getenv('BASE_PATH')

if user_choice == "BRONZE":
    runner = dbx_main()
elif user_choice == "Create MOP":
    print("Creating MOP...")
    create_mop()
elif user_choice == "Create Sharepoint":
    print("Create Sharepoint...")
    open_link_safe("https://pldt365.sharepoint.com/sites/ITDataTechnologies/DataEngineering/SitePages/GDM-Alapaap.aspx")
else:
    print(f"That feature(s) {user_choice} is not yet available!")