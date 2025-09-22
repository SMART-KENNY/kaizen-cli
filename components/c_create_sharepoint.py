import webbrowser
import re

def open_link_safe(url: str) -> bool:
    """
    Safely open a URL in the default web browser.
    
    Args:
        url (str): The URL to open.
    
    Returns:
        bool: True if the URL was opened, False otherwise.
    """
    # Simple regex to validate URL format
    url_pattern = re.compile(r"^https?://[^\s/$.?#].[^\s]*$", re.IGNORECASE)
    
    if not url_pattern.match(url):
        print(f"❌ Invalid or unsafe URL: {url}")
        return False
    
    try:
        webbrowser.open(url, new=2)  # new=2 -> open in a new tab
        print(f"✅ Opened link: {url}")
        return True
    except Exception as e:
        print(f"❌ Failed to open link: {e}")
        return False



if __name__ == "__main__":

    if open_link_safe():
        print("Link opened successfully")
    else:
        print("Could not open link")