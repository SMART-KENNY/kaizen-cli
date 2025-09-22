from InquirerPy import inquirer


def ai_options():

    pick = inquirer.select(
        message="I got you, what do you want? :",
        choices=["Ask / prompt AI",  "Run MCP", "Run databricks generator!", "Create MOP", "Create Sharepoint"],
        default="bright",
    ).execute()

    return pick

if __name__ == "__main__":
    ai_options()