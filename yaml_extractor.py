#!/usr/bin/env python3
"""
Simple Interactive YAML Extractor
"""

import yaml
import json

def simple_yaml_extractor():
    # Get file path
    file_path = input("Enter YAML file path: ").strip()
    
    try:
        # Load YAML
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
        
        print("\n✅ YAML loaded successfully!")
        
        while True:
            print("\n" + "="*50)
            print("YAML Extractor Options:")
            print("1. Get specific key value")
            print("2. Get all subchildren of a key")
            print("3. Show all keys")
            print("4. Search keys")
            print("5. Exit")
            
            choice = input("\nSelect option (1-5): ").strip()
            
            if choice == '1':
                key = input("Enter key path (use dots for nested keys): ").strip()
                result = get_nested_value(data, key)
                print(f"\nResult: {json.dumps(result, indent=2)}")
            
            elif choice == '2':
                parent = input("Enter parent key: ").strip()
                result = get_nested_value(data, parent)
                if isinstance(result, dict):
                    print(f"\nSubchildren of '{parent}':")
                    print(json.dumps(result, indent=2))
                else:
                    print(f"\n'{parent}' value: {result}")
            
            elif choice == '3':
                keys = get_all_keys(data)
                print("\nAll available keys:")
                for key in keys:
                    print(f"  • {key}")
            
            elif choice == '4':
                search_term = input("Enter search term: ").strip()
                keys = get_all_keys(data)
                matches = [k for k in keys if search_term.lower() in k.lower()]
                print(f"\nKeys containing '{search_term}':")
                for key in matches:
                    value = get_nested_value(data, key)
                    print(f"  • {key}: {value}")
            
            elif choice == '5':
                print("Goodbye!")
                break
            
            else:
                print("Invalid option. Please try again.")
    
    except FileNotFoundError:
        print(f"❌ File '{file_path}' not found!")
    except Exception as e:
        print(f"❌ Error: {e}")

def get_nested_value(data, key_path):
    """Get value from nested dictionary using dot notation"""
    keys = key_path.split('.')
    current = data
    
    try:
        for key in keys:
            current = current[key]
        return current
    except (KeyError, TypeError):
        return f"Key '{key_path}' not found"

def get_all_keys(data, parent_key=''):
    """Get all keys from nested dictionary"""
    keys = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            current_key = f"{parent_key}.{key}" if parent_key else key
            keys.append(current_key)
            
            if isinstance(value, dict):
                keys.extend(get_all_keys(value, current_key))
    
    return keys

if __name__ == "__main__":
    simple_yaml_extractor()