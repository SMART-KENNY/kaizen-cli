import os
import subprocess
from dotenv import load_dotenv

def open_file_maximized(file_path: str):
    # Configure startup info
    startup_info = subprocess.STARTUPINFO()
    startup_info.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startup_info.wShowWindow = 3

    # Open file with default app, maximized
    subprocess.Popen(
        ["start", "", file_path],
        shell=True,
        startupinfo=startup_info
    )

if __name__ == "__main__":
    load_dotenv()
    BASE_PATH = os.getenv('BASE_PATH')
    open_file_maximized(rf"{BASE_PATH}output\instructions.txt")
