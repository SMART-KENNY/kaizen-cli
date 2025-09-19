import json
from typing import Dict, List, Any, Union, Set
import argparse
import sys
import os

class JSONKeyExtractor:
    """
    A class to extract keys from JSON data with various options.
    """
    
    def __init__(self):
        self.keys: Set[str] = set()
        self.paths: List[str] = []
        self.key_types: Dict[str, str] = {}
    
    def extract_keys(self, data: Union[str, Dict, List], current_path: str = "") -> None:
        """
        Extract keys from JSON data recursively.
        
        Args:
            data: JSON string or parsed JSON object
            current_path: Current path in the JSON structure
        """
        # Parse JSON string if needed
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON: {e}")
        
        self._extract_recursive(data, current_path)
    
    def _extract_recursive(self, obj: Any, current_path: str = "") -> None:
        """
        Recursively extract keys from nested structures.
        """
        if obj is None:
            return
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_path = f"{current_path}.{key}" if current_path else key
                
                self.keys.add(key)
                self.paths.append(new_path)
                self.key_types[new_path] = type(value).__name__
                
                # Recursively process nested objects
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
                 filter_by_type: str = None) -> List[str]:
        """
        Get extracted keys with various formatting options.
        
        Args:
            include_nested: Include keys from nested objects
            show_path: Show full path instead of just key names
            unique_only: Return only unique keys
            sort_keys: Sort keys alphabetically
            filter_by_type: Filter by value type (str, int, dict, list, etc.)
        
        Returns:
            List of extracted keys
        """
        if show_path:
            result = self.paths.copy()
            
            if filter_by_type:
                result = [path for path in result 
                         if self.key_types.get(path, '').lower() == filter_by_type.lower()]
        else:
            if unique_only:
                result = list(self.keys)
            else:
                # Extract just the key names from paths
                result = []
                for path in self.paths:
                    key = path.split('.')[-1].split('[')[0]
                    result.append(key)
        
        if sort_keys:
            result.sort()
        
        return result
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the extracted keys.
        
        Returns:
            Dictionary with statistics
        """
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
        """Reset the extractor for new data."""
        self.keys.clear()
        self.paths.clear()
        self.key_types.clear()
    
    def export_results(self, 
                      format_type: str = 'list',
                      include_types: bool = False,
                      **kwargs) -> Union[List[str], Dict[str, Any], str]:
        """
        Export results in different formats.
        
        Args:
            format_type: 'list', 'dict', 'json', or 'csv'
            include_types: Include type information
            **kwargs: Additional options for get_keys()
        
        Returns:
            Formatted results
        """
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


def interactive_mode():
    """Interactive mode for the JSON Key Extractor."""
    print("=== JSON Key Extractor - Interactive Mode ===")
    print("Enter 'quit' or 'exit' to stop, 'help' for commands\n")
    
    extractor = JSONKeyExtractor()
    
    while True:
        try:
            command = input("json-extractor> ").strip()
            
            if command.lower() in ['quit', 'exit', 'q']:
                print("Goodbye!")
                break
            
            elif command.lower() == 'help':
                print_help()
                continue
            
            elif command.lower() == 'clear':
                extractor.reset()
                print("Extractor reset.")
                continue
            
            elif command.lower() == 'stats':
                if not extractor.paths:
                    print("No data loaded. Please extract keys first.")
                else:
                    stats = extractor.get_statistics()
                    print_statistics(stats)
                continue
            
            elif command.startswith('load '):
                filename = command[5:].strip()
                try:
                    with open(filename, 'r') as f:
                        json_data = f.read()
                    extractor.reset()
                    extractor.extract_keys(json_data)
                    print(f"Loaded JSON from {filename}")
                    print(f"Found {len(extractor.paths)} keys")
                except FileNotFoundError:
                    print(f"File not found: {filename}")
                except Exception as e:
                    print(f"Error loading file: {e}")
                continue
            
            elif command.startswith('save '):
                filename = command[5:].strip()
                if not extractor.paths:
                    print("No data to save. Please extract keys first.")
                else:
                    try:
                        keys = extractor.get_keys(sort_keys=True)
                        with open(filename, 'w') as f:
                            f.write('\n'.join(keys))
                        print(f"Keys saved to {filename}")
                    except Exception as e:
                        print(f"Error saving file: {e}")
                continue
            
            elif command.lower() == 'show':
                if not extractor.paths:
                    print("No data loaded. Please extract keys first.")
                else:
                    keys = extractor.get_keys(sort_keys=True)
                    print(f"\nExtracted Keys ({len(keys)} total):")
                    for i, key in enumerate(keys, 1):
                        print(f"{i:3d}. {key}")
                    print()
                continue
            
            elif command.lower() == 'paths':
                if not extractor.paths:
                    print("No data loaded. Please extract keys first.")
                else:
                    paths = extractor.get_keys(show_path=True, sort_keys=True)
                    print(f"\nKey Paths ({len(paths)} total):")
                    for i, path in enumerate(paths, 1):
                        print(f"{i:3d}. {path}")
                    print()
                continue
            
            # Try to parse as JSON
            try:
                extractor.reset()
                extractor.extract_keys(command)
                keys = extractor.get_keys(sort_keys=True)
                print(f"\nExtracted {len(keys)} keys:")
                for key in keys:
                    print(f"  - {key}")
                print()
                
            except ValueError as e:
                print(f"Error: {e}")
                print("Try 'help' for available commands.")
        
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except EOFError:
            print("\nGoodbye!")
            break


def print_help():
    """Print help information."""
    help_text = """
Available commands:
  help                 - Show this help message
  clear                - Reset the extractor
  stats                - Show statistics about extracted keys
  show                 - Display all extracted keys
  paths                - Display full key paths
  load <filename>      - Load JSON from file
  save <filename>      - Save extracted keys to file
  quit/exit/q          - Exit the program
  
  Or enter JSON directly to extract keys immediately.
  
Examples:
  {"name": "John", "age": 30}
  load data.json
  save keys.txt
"""
    print(help_text)


def print_statistics(stats: Dict[str, Any]):
    """Print formatted statistics."""
    print("\n=== Statistics ===")
    print(f"Total keys: {stats['total_keys']}")
    print(f"Unique keys: {stats['unique_keys']}")
    print(f"Max depth: {stats['max_depth']}")
    print("\nType distribution:")
    for type_name, count in sorted(stats['type_distribution'].items()):
        print(f"  {type_name}: {count}")
    print()


def main():
    """Command line interface for the JSON Key Extractor."""
    parser = argparse.ArgumentParser(
        description='Extract keys from JSON data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python json_extractor.py '{"name": "John", "age": 30}'
  python json_extractor.py --file data.json --sort --show-path
  python json_extractor.py --interactive
  echo '{"a": 1}' | python json_extractor.py --stdin
        """
    )
    
    # Input options
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument('input', nargs='?', help='JSON string')
    input_group.add_argument('--file', '-f', help='Read JSON from file')
    input_group.add_argument('--stdin', action='store_true', help='Read JSON from stdin')
    input_group.add_argument('--interactive', '-i', action='store_true', 
                           help='Start interactive mode')
    
    # Output options
    parser.add_argument('--output', '-o', help='Output file path')
    parser.add_argument('--format', choices=['list', 'dict', 'json', 'csv'], 
                       default='list', help='Output format (default: list)')
    
    # Extraction options
    parser.add_argument('--show-path', action='store_true', 
                       help='Show full paths instead of key names')
    parser.add_argument('--include-types', action='store_true',
                       help='Include type information')
    parser.add_argument('--sort', action='store_true', 
                       help='Sort keys alphabetically')
    parser.add_argument('--no-unique', action='store_true',
                       help='Allow duplicate keys')
    parser.add_argument('--filter-type', help='Filter by value type')
    
    # Display options
    parser.add_argument('--stats', action='store_true',
                       help='Show statistics')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Suppress informational messages')
    
    args = parser.parse_args()
    
    # Interactive mode
    if args.interactive:
        interactive_mode()
        return
    
    # Get JSON data
    json_data = None
    
    if args.stdin:
        json_data = sys.stdin.read().strip()
    elif args.file:
        if not os.path.exists(args.file):
            print(f"Error: File not found: {args.file}", file=sys.stderr)
            sys.exit(1)
        try:
            with open(args.file, 'r', encoding='utf-8') as f:
                json_data = f.read()
        except Exception as e:
            print(f"Error reading file: {e}", file=sys.stderr)
            sys.exit(1)
    elif args.input:
        json_data = args.input
    else:
        parser.print_help()
        return
    
    if not json_data or not json_data.strip():
        print("Error: No JSON data provided", file=sys.stderr)
        sys.exit(1)
    
    # Extract keys
    extractor = JSONKeyExtractor()
    try:
        extractor.extract_keys(json_data)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    if not extractor.paths:
        if not args.quiet:
            print("No keys found in the JSON data")
        return
    
    # Get results
    try:
        results = extractor.export_results(
            format_type=args.format,
            show_path=args.show_path,
            include_types=args.include_types,
            sort_keys=args.sort,
            unique_only=not args.no_unique,
            filter_by_type=args.filter_type
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Output results
    output_text = ""
    if isinstance(results, list):
        output_text = '\n'.join(results)
    else:
        output_text = str(results)
    
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(output_text)
            if not args.quiet:
                print(f"Results saved to {args.output}")
        except Exception as e:
            print(f"Error writing to file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(output_text)
    
    # Show statistics if requested
    if args.stats:
        stats = extractor.get_statistics()
        print_statistics(stats)


# Example usage functions
def example_usage():
    """Demonstrate various usage examples."""
    print("=== JSON Key Extractor Examples ===\n")
    
    # Example 1: Simple JSON
    print("Example 1: Simple JSON object")
    simple_json = '{"name": "John", "age": 30, "city": "New York"}'
    extractor = JSONKeyExtractor()
    extractor.extract_keys(simple_json)
    keys = extractor.get_keys(sort_keys=True)
    print(f"JSON: {simple_json}")
    print(f"Keys: {keys}\n")
    
    # Example 2: Nested JSON
    print("Example 2: Nested JSON object")
    nested_json = '''
    {
        "user": {
            "name": "Alice",
            "profile": {
                "age": 25,
                "interests": ["reading", "coding"]
            }
        },
        "settings": {
            "theme": "dark",
            "notifications": true
        }
    }
    '''
    extractor.reset()
    extractor.extract_keys(nested_json)
    
    print("All unique keys:")
    keys = extractor.get_keys(sort_keys=True)
    for key in keys:
        print(f"  - {key}")
    
    print("\nFull paths:")
    paths = extractor.get_keys(show_path=True, sort_keys=True)
    for path in paths:
        print(f"  - {path}")
    
    print("\nStatistics:")
    stats = extractor.get_statistics()
    print_statistics(stats)


if __name__ == "__main__":
    # If no arguments provided, show examples
    if len(sys.argv) == 1:
        print("JSON Key Extractor")
        print("Use --help for usage information or --interactive for interactive mode")
        print("Use --examples to see usage examples\n")
        
        # Show a quick example
        extractor = JSONKeyExtractor()
        sample_json = '{"name": "John", "age": 30, "address": {"city": "NYC", "zip": "10001"}}'
        extractor.extract_keys(sample_json)
        keys = extractor.get_keys(sort_keys=True)
        
        print(f"Quick example:")
        print(f"Input:  {sample_json}")
        print(f"Output: {keys}")
        print("\nTry: python json_extractor.py --interactive")
    
    elif len(sys.argv) == 2 and sys.argv[1] == '--examples':
        example_usage()
    
    else:
        main()