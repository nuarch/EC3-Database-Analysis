
import json
import os
import sys
from datetime import datetime
from collections import defaultdict
import re

def load_json_data(file_path):
    """Load JSON data from file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return None

def get_available_schemas(functions):
    """Get list of all available schemas from the functions data"""
    schemas = set()
    for func in functions:
        schema = func['function_info']['schema']
        schemas.add(schema)
    return sorted(list(schemas))

def select_schemas_interactive(available_schemas):
    """Interactive schema selection"""
    print("\nAvailable schemas:")
    for i, schema in enumerate(available_schemas, 1):
        print(f"{i}. {schema}")
    
    print(f"\n{len(available_schemas) + 1}. All schemas")
    print("0. Exit")
    
    while True:
        try:
            choice = input(f"\nSelect schemas (comma-separated numbers, or single number): ").strip()
            
            if choice == '0':
                return None
            
            if choice == str(len(available_schemas) + 1):
                return available_schemas
            
            # Parse comma-separated choices
            selected_indices = []
            for item in choice.split(','):
                item = item.strip()
                if item:
                    idx = int(item)
                    if 1 <= idx <= len(available_schemas):
                        selected_indices.append(idx - 1)
                    else:
                        print(f"Invalid choice: {idx}. Please select between 1 and {len(available_schemas)}")
                        break
            else:
                # All choices were valid
                return [available_schemas[i] for i in selected_indices]
                
        except ValueError:
            print("Invalid input. Please enter numbers separated by commas.")
        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            return None

def create_safe_filename(schema_name, function_name):
    """Create a safe filename from schema and function names"""
    # Remove or replace characters that are problematic in filenames
    safe_schema = re.sub(r'[<>:"/\\|?*]', '_', schema_name)
    safe_function = re.sub(r'[<>:"/\\|?*]', '_', function_name)
    return f"{safe_schema} - {safe_function}"

def convert_markdown_to_adf(markdown_text):
    """
    Convert markdown text to Atlassian Document Format (ADF).
    
    Args:
        markdown_text (str): The markdown text to convert
        
    Returns:
        dict: The ADF document structure
    """
    # Initialize ADF document structure
    adf_doc = {
        "version": 1,
        "type": "doc",
        "content": []
    }
    
    lines = markdown_text.split('\n')
    current_content = []
    
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        
        # Skip empty lines
        if not line:
            i += 1
            continue
        
        # Handle headers
        if line.startswith('#'):
            # First, add any pending paragraph content
            if current_content:
                para_content = _create_paragraph_content(' '.join(current_content))
                if para_content:
                    adf_doc["content"].append(_create_paragraph(para_content))
                current_content = []
            
            # Determine header level
            level = len(line) - len(line.lstrip('#'))
            header_text = line.lstrip('#').strip()
            
            adf_doc["content"].append(_create_heading(header_text, level))
        
        # Handle code blocks
        elif line.startswith('```'):
            # First, add any pending paragraph content
            if current_content:
                para_content = _create_paragraph_content(' '.join(current_content))
                if para_content:
                    adf_doc["content"].append(_create_paragraph(para_content))
                current_content = []

            # Extract content after the opening ``` (if any)
            opening_line = line[3:].strip()  # Remove ``` and get remainder

            # Find the end of the code block
            code_lines = []

            # Add opening line content if it exists
            if opening_line:
                code_lines.append(opening_line)

            i += 1
            while i < len(lines):
                current_line = lines[i]

                # Check if this line ends the code block
                if current_line.rstrip().endswith('```'):
                    # Extract content before the closing ```
                    closing_content = current_line[:-3].rstrip()
                    if closing_content:
                        code_lines.append(closing_content)
                    break
                elif current_line.startswith('```'):
                    # Line starts with ``` (alternative closing pattern)
                    break
                else:
                    # Regular code line
                    code_lines.append(current_line)

                i += 1

            code_content = '\n'.join(code_lines)
            adf_doc["content"].append(_create_code_block(code_content, "sql"))

        
        # Handle unordered lists
        elif _is_bullet_list_item(line):
            # First, add any pending paragraph content
            if current_content:
                para_content = _create_paragraph_content(' '.join(current_content))
                if para_content:
                    adf_doc["content"].append(_create_paragraph(para_content))
                current_content = []
            
            # Parse the entire nested list structure
            list_structure, items_processed = _parse_nested_bullet_list(lines, i)
            adf_doc["content"].append(_create_nested_bullet_list(list_structure))
            i += items_processed - 1  # Adjust for items processed
        
        # Handle numbered lists
        elif _is_numbered_list_item(line):
            # First, add any pending paragraph content
            if current_content:
                para_content = _create_paragraph_content(' '.join(current_content))
                if para_content:
                    adf_doc["content"].append(_create_paragraph(para_content))
                current_content = []
            
            # Parse the entire nested numbered list structure
            list_structure, items_processed = _parse_nested_numbered_list(lines, i)
            adf_doc["content"].append(_create_nested_numbered_list(list_structure))
            i += items_processed - 1  # Adjust for items processed
        
        # Regular text - accumulate for paragraphs
        else:
            current_content.append(line)
        
        i += 1
    
    # Add any remaining paragraph content
    if current_content:
        para_content = _create_paragraph_content(' '.join(current_content))
        if para_content:
            adf_doc["content"].append(_create_paragraph(para_content))
    
    return adf_doc

def _is_bullet_list_item(line):
    """Check if line is a bullet list item at any indentation level"""
    stripped = line.lstrip()
    return stripped.startswith('- ') or stripped.startswith('* ')

def _is_numbered_list_item(line):
    """Check if line is a numbered list item at any indentation level"""
    stripped = line.lstrip()
    return re.match(r'^\d+\.\s+', stripped) is not None

def _get_indentation_level(line):
    """Get the indentation level of a line (number of leading spaces)"""
    return len(line) - len(line.lstrip())

def _parse_nested_bullet_list(lines, start_index):
    """
    Parse a nested bullet list structure starting from start_index.
    Returns (list_structure, items_processed)
    """
    items = []
    i = start_index
    base_indent = _get_indentation_level(lines[start_index])
    
    while i < len(lines):
        line = lines[i]
        
        # Skip empty lines
        if not line.strip():
            i += 1
            continue
        
        current_indent = _get_indentation_level(line)
        
        # If indentation is less than base level, we're done with this list
        if current_indent < base_indent:
            break
        
        # If this is a bullet list item at the current level
        if current_indent == base_indent and _is_bullet_list_item(line):
            stripped = line.lstrip()
            item_text = stripped[2:].strip()  # Remove '- ' or '* '
            
            # Look ahead for nested items
            nested_content = []
            j = i + 1
            
            while j < len(lines):
                next_line = lines[j]
                
                # Skip empty lines
                if not next_line.strip():
                    j += 1
                    continue
                
                next_indent = _get_indentation_level(next_line)
                
                # If next line is at same or lower indentation and is a list item, stop
                if (next_indent <= current_indent and 
                    (_is_bullet_list_item(next_line) or _is_numbered_list_item(next_line))):
                    break
                
                # If next line is indented more, it's nested content
                if next_indent > current_indent:
                    # Check if it's a nested bullet list
                    if _is_bullet_list_item(next_line):
                        nested_list, nested_processed = _parse_nested_bullet_list(lines, j)
                        nested_content.append({
                            'type': 'bulletList',
                            'content': nested_list
                        })
                        j += nested_processed
                    # Check if it's a nested numbered list
                    elif _is_numbered_list_item(next_line):
                        nested_list, nested_processed = _parse_nested_numbered_list(lines, j)
                        nested_content.append({
                            'type': 'orderedList',
                            'content': nested_list
                        })
                        j += nested_processed
                    else:
                        # Regular text content - add to current item text
                        item_text += ' ' + next_line.strip()
                        j += 1
                else:
                    break
            
            items.append({
                'text': item_text,
                'nested_content': nested_content
            })
            
            i = j
        else:
            # If we encounter a line that doesn't fit the pattern, break
            break
    
    return items, i - start_index

def _parse_nested_numbered_list(lines, start_index):
    """
    Parse a nested numbered list structure starting from start_index.
    Returns (list_structure, items_processed)
    """
    items = []
    i = start_index
    base_indent = _get_indentation_level(lines[start_index])
    
    while i < len(lines):
        line = lines[i]
        
        # Skip empty lines
        if not line.strip():
            i += 1
            continue
        
        current_indent = _get_indentation_level(line)
        
        # If indentation is less than base level, we're done with this list
        if current_indent < base_indent:
            break
        
        # If this is a numbered list item at the current level
        if current_indent == base_indent and _is_numbered_list_item(line):
            stripped = line.lstrip()
            item_text = re.sub(r'^\d+\.\s+', '', stripped)
            
            # Look ahead for nested items
            nested_content = []
            j = i + 1
            
            while j < len(lines):
                next_line = lines[j]
                
                # Skip empty lines
                if not next_line.strip():
                    j += 1
                    continue
                
                next_indent = _get_indentation_level(next_line)
                
                # If next line is at same or lower indentation and is a list item, stop
                if (next_indent <= current_indent and 
                    (_is_bullet_list_item(next_line) or _is_numbered_list_item(next_line))):
                    break
                
                # If next line is indented more, it's nested content
                if next_indent > current_indent:
                    # Check if it's a nested bullet list
                    if _is_bullet_list_item(next_line):
                        nested_list, nested_processed = _parse_nested_bullet_list(lines, j)
                        nested_content.append({
                            'type': 'bulletList',
                            'content': nested_list
                        })
                        j += nested_processed
                    # Check if it's a nested numbered list
                    elif _is_numbered_list_item(next_line):
                        nested_list, nested_processed = _parse_nested_numbered_list(lines, j)
                        nested_content.append({
                            'type': 'orderedList',
                            'content': nested_list
                        })
                        j += nested_processed
                    else:
                        # Regular text content - add to current item text
                        item_text += ' ' + next_line.strip()
                        j += 1
                else:
                    break
            
            items.append({
                'text': item_text,
                'nested_content': nested_content
            })
            
            i = j
        else:
            # If we encounter a line that doesn't fit the pattern, break
            break
    
    return items, i - start_index

def _create_nested_bullet_list(items):
    """Create ADF bullet list node with proper nesting support"""
    list_items = []
    
    for item in items:
        item_content = []
        
        # Add main item content
        para_content = _create_paragraph_content(item["text"])
        if para_content:
            item_content.append(_create_paragraph(para_content))
        
        # Add nested content
        for nested in item.get("nested_content", []):
            if nested['type'] == 'bulletList':
                item_content.append(_create_nested_bullet_list(nested['content']))
            elif nested['type'] == 'orderedList':
                item_content.append(_create_nested_numbered_list(nested['content']))
        
        list_items.append({
            "type": "listItem",
            "content": item_content
        })
    
    return {
        "type": "bulletList",
        "content": list_items
    }

def _create_nested_numbered_list(items):
    """Create ADF numbered list node with proper nesting support"""
    list_items = []
    
    for item in items:
        item_content = []
        
        # Add main item content
        para_content = _create_paragraph_content(item["text"])
        if para_content:
            item_content.append(_create_paragraph(para_content))
        
        # Add nested content
        for nested in item.get("nested_content", []):
            if nested['type'] == 'bulletList':
                item_content.append(_create_nested_bullet_list(nested['content']))
            elif nested['type'] == 'orderedList':
                item_content.append(_create_nested_numbered_list(nested['content']))
        
        list_items.append({
            "type": "listItem",
            "content": item_content
        })
    
    return {
        "type": "orderedList",
        "content": list_items
    }

def _create_heading(text, level):
    """Create ADF heading node"""
    return {
        "type": "heading",
        "attrs": {
            "level": min(level, 6)  # ADF supports levels 1-6
        },
        "content": [
            {
                "type": "text",
                "text": text
            }
        ]
    }

def _create_paragraph(content):
    """Create ADF paragraph node"""
    return {
        "type": "paragraph",
        "content": content
    }

def _create_paragraph_content(text):
    """Create paragraph content with inline formatting"""
    if not text.strip():
        return []
    
    content = []
    parts = _split_text_with_formatting(text)
    
    for part in parts:
        if part["type"] == "text":
            content.append({
                "type": "text",
                "text": part["text"]
            })
        elif part["type"] == "strong":
            content.append({
                "type": "text",
                "text": part["text"],
                "marks": [{"type": "strong"}]
            })
        elif part["type"] == "em":
            content.append({
                "type": "text",
                "text": part["text"],
                "marks": [{"type": "em"}]
            })
        elif part["type"] == "code":
            content.append({
                "type": "text",
                "text": part["text"],
                "marks": [{"type": "code"}]
            })
    
    return content

def _split_text_with_formatting(text):
    """Split text into parts with formatting information"""
    parts = []
    current_pos = 0

    # Find all formatting patterns - ORDER MATTERS: more specific patterns first
    patterns = [
        (r'\*\*(.*?)\*\*', 'strong'),  # Bold (must come before italic)
        (r'\*(.*?)\*', 'em'),          # Italic
        (r'`(.*?)`', 'code')           # Inline code
    ]

    matches = []
    for pattern, format_type in patterns:
        for match in re.finditer(pattern, text):
            matches.append({
                'start': match.start(),
                'end': match.end(),
                'text': match.group(1),
                'type': format_type,
                'full_match': match.group(0)
            })

    # Sort matches by start position
    matches.sort(key=lambda x: x['start'])

    # Remove overlapping matches (keep the first one found)
    filtered_matches = []
    for match in matches:
        # Check if this match overlaps with any already accepted match
        overlaps = False
        for existing in filtered_matches:
            if (match['start'] < existing['end'] and match['end'] > existing['start']):
                overlaps = True
                break

        if not overlaps:
            filtered_matches.append(match)

    # Process non-overlapping matches
    for match in filtered_matches:
        # Add text before the match
        if current_pos < match['start']:
            before_text = text[current_pos:match['start']]
            if before_text:
                parts.append({
                    'type': 'text',
                    'text': before_text
                })

        # Add the formatted text
        parts.append({
            'type': match['type'],
            'text': match['text']
        })

        current_pos = match['end']

    # Add remaining text
    if current_pos < len(text):
        remaining_text = text[current_pos:]
        if remaining_text:
            parts.append({
                'type': 'text',
                'text': remaining_text
            })

    # If no formatting found, return the whole text
    if not parts:
        parts.append({
            'type': 'text',
            'text': text
        })

    return parts

def _create_code_block(code, language="sql"):
    """Create ADF code block node"""
    return {
        "type": "codeBlock",
        "attrs": {
            "language": language
        },
        "content": [
            {
                "type": "text",
                "text": code
            }
        ]
    }

def _create_bullet_list(items):
    """Create ADF bullet list node"""
    list_items = []
    
    for item in items:
        item_content = []
        
        # Add main item content
        para_content = _create_paragraph_content(item["text"])
        if para_content:
            item_content.append(_create_paragraph(para_content))
        
        # Add nested ordered list if present
        if item.get("nested"):
            nested_items = []
            for nested_text in item["nested"]:
                nested_para_content = _create_paragraph_content(nested_text)
                if nested_para_content:
                    nested_items.append({
                        "type": "listItem",
                        "content": [_create_paragraph(nested_para_content)]
                    })
            
            if nested_items:
                item_content.append({
                    "type": "orderedList",
                    "content": nested_items
                })
        
        list_items.append({
            "type": "listItem",
            "content": item_content
        })
    
    return {
        "type": "bulletList",
        "content": list_items
    }

def _create_numbered_list(items):
    """Create ADF numbered list node"""
    list_items = []
    
    for item_text in items:
        para_content = _create_paragraph_content(item_text)
        if para_content:
            list_items.append({
                "type": "listItem",
                "content": [_create_paragraph(para_content)]
            })
    
    return {
        "type": "orderedList",
        "content": list_items
    }

def format_confluence_content(text):
    """
    Format Markdown text content for Confluence ADF format.
    
    Args:
        text (str): The text content to format
        
    Returns:
        dict: The formatted Confluence ADF document
    """
    return convert_markdown_to_adf(text)

def create_function_metadata(func):
    """Create metadata JSON for a function"""
    func_info = func['function_info']
    analysis = func.get('analysis', {}) or func.get('chatgpt_explanation', {})
    
    schema_name = func_info['schema']
    function_name = func_info['name']
    
    # Get complexity from analysis
    complexity = 'N/A'
    if isinstance(analysis, dict):
        if 'complexity' in analysis and analysis['complexity']:
            complexity = analysis['complexity']
        elif 'complexity_score' in analysis and analysis['complexity_score']:
            complexity = analysis['complexity_score']
    
    # Basic metadata
    metadata = {
        "title": f"{schema_name} - {function_name}",
        "function_name": function_name,
        "schema": schema_name,
        "type": "Function",
        "complexity": complexity,
        "generated_date": datetime.now().isoformat(),
        "description": f"Analysis and documentation for function {function_name} in schema {schema_name}"
    }
    
    # Add additional metadata from function info
    if func_info.get('created_date'):
        metadata['created_date'] = func_info['created_date']
    
    if func_info.get('modified_date'):
        metadata['modified_date'] = func_info['modified_date']
    
    if func_info.get('description'):
        metadata['description'] = func_info['description']
    
    # Add return type if available
    if func_info.get('return_type'):
        metadata['return_type'] = func_info['return_type']
    
    # Add analysis metadata if available
    if isinstance(analysis, dict):
        if analysis.get('purpose'):
            metadata['purpose'] = analysis['purpose']
        
        if analysis.get('returns'):
            metadata['returns'] = analysis['returns']
        
        if analysis.get('business_logic'):
            metadata['business_logic_summary'] = analysis['business_logic'][:200] + "..." if len(str(analysis['business_logic'])) > 200 else analysis['business_logic']
    
    return metadata

def _extract_last_heading_number(text):
    """Extract the first heading number from the last section"""
    # Split into sections by main headings
    sections = re.split(r'\n#[^#]', text)
    if not sections:
        return 0

    return len(sections)

def generate_function_page(func):
    """Generate Confluence ADF content for a single function"""
    func_info = func['function_info']
    analysis = func.get('analysis', {}) or func.get('chatgpt_explanation', {})

    schema_name = func_info['schema']
    function_name = func_info['name']

    content = ''

    # Analysis sections
    if isinstance(analysis, dict):
        # Detailed explanation
        if analysis.get('explanation'):
            # Remove some content that is not needed
            text = analysis['explanation']
            # text = re.sub(r'#\s+\d+\.', '#', text, flags=re.MULTILINE)

            # Promote all headings up three levels (remove one # from each heading)
            # Process from most specific to least specific to avoid conflicts
            text = re.sub(r'^#### (.*?)$', r'# \1', text, flags=re.MULTILINE)  # h4 -> h1
            text = re.sub(r'^##### (.*?)$', r'## \1', text, flags=re.MULTILINE)  # h5 -> h2
            text = re.sub(r'^###### (.*?)$', r'### \1', text, flags=re.MULTILINE)  # h6 -> h3

            content += text

    # Extract the last heading number from the explanation
    last_heading_number = _extract_last_heading_number(content)

    # Function Definition/Source Code
    definition_field = func_info.get('definition') or func_info.get('source_code')
    if definition_field:
        content += f"\n\n# {last_heading_number + 1}. Function Definition\n\n"
        content += '```' + definition_field + '```\n'

    # Convert to ADF format
    adf_content = format_confluence_content(content)
    
    return adf_content

def generate_function_confluence_files(json_file_path, output_dir="./confluence_docs/funcs", selected_schemas=None):
    """Generate separate Confluence ADF files and metadata for each function"""
    
    # Load JSON data
    functions = load_json_data(json_file_path)
    if not functions:
        print("Failed to load JSON data")
        return False
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    # Filter functions by selected schemas if specified
    if selected_schemas:
        filtered_functions = []
        for func in functions:
            schema = func['function_info']['schema']
            if schema in selected_schemas:
                filtered_functions.append(func)
        functions = filtered_functions
    
    if not functions:
        print("No functions to process")
        return False
    
    generated_files = []
    schema_counts = defaultdict(int)
    
    # Generate Confluence file and metadata for each function
    for func in functions:
        func_info = func['function_info']
        schema_name = func_info['schema']
        function_name = func_info['name']
        
        # Generate Confluence ADF content
        adf_content = generate_function_page(func)
        
        # Create metadata
        metadata = create_function_metadata(func)
        
        # Create filename base - keeping original capitalization
        filename_base = create_safe_filename(schema_name, function_name)
        adf_filename = f"{filename_base}.json"  # ADF content in JSON format
        metadata_filename = f"{filename_base}_metadata.json"  # Separate metadata file
        
        adf_output_file = os.path.join(output_dir, adf_filename)
        metadata_output_file = os.path.join(output_dir, metadata_filename)
        
        # Count functions per schema for summary
        schema_counts[schema_name] += 1
        
        # Write ADF file
        try:
            with open(adf_output_file, 'w', encoding='utf-8') as file:
                json.dump(adf_content, file, indent=2, ensure_ascii=False)
            print(f"Generated ADF: {adf_filename}")
            generated_files.append(adf_output_file)
        except Exception as e:
            print(f"Error writing ADF file {adf_output_file}: {e}")
            return False
        
        # Write metadata file
        try:
            with open(metadata_output_file, 'w', encoding='utf-8') as file:
                json.dump(metadata, file, indent=2, ensure_ascii=False)
            print(f"Generated metadata: {metadata_filename}")
            generated_files.append(metadata_output_file)
        except Exception as e:
            print(f"Error writing metadata file {metadata_output_file}: {e}")
            return False
    
    # Print summary
    print(f"\nSuccessfully generated {len(generated_files)} files ({len(generated_files)//2} functions):")
    print("\nFunctions by schema:")
    for schema, count in sorted(schema_counts.items()):
        print(f"  {schema}: {count} functions")
    
    return True

def parse_command_line_args():
    """Parse command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate individual Confluence pages and metadata for functions')
    parser.add_argument('--input', '-i', default='./export/functions_analysis_all_schemas.json',
                        help='Input JSON file path (default: ./export/functions_analysis_all_schemas.json)')
    parser.add_argument('--output', '-o', default='./confluence_docs/funcs',
                        help='Output directory (default: ./confluence_docs/funcs)')
    parser.add_argument('--schemas', '-s', nargs='*',
                        help='Specific schemas to process (space-separated). If not provided, interactive selection will be used.')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Process all schemas without interactive selection')
    
    return parser.parse_args()

def main():
    """Main function"""
    args = parse_command_line_args()
    
    json_file = args.input
    output_dir = args.output
    
    # Check if a JSON file exists
    if not os.path.exists(json_file):
        print(f"JSON file not found: {json_file}")
        return
    
    # Load JSON data to get available schemas
    functions = load_json_data(json_file)
    if not functions:
        print("Failed to load JSON data")
        return
    
    available_schemas = get_available_schemas(functions)
    
    if not available_schemas:
        print("No schemas found in the data")
        return
    
    selected_schemas = None
    
    # Determine which schemas to process
    if args.all:
        # Process all schemas
        selected_schemas = available_schemas
        print(f"Processing all {len(available_schemas)} schemas")
    elif args.schemas:
        # Use command line specified schemas
        selected_schemas = []
        for schema in args.schemas:
            if schema in available_schemas:
                selected_schemas.append(schema)
            else:
                print(f"Warning: Schema '{schema}' not found. Available schemas: {', '.join(available_schemas)}")
        
        if not selected_schemas:
            print("No valid schemas specified")
            return
            
        print(f"Processing {len(selected_schemas)} specified schemas: {', '.join(selected_schemas)}")
    else:
        # Interactive selection
        selected_schemas = select_schemas_interactive(available_schemas)
        if selected_schemas is None:
            print("Operation cancelled")
            return
        
        if len(selected_schemas) == len(available_schemas):
            print(f"Processing all {len(selected_schemas)} schemas")
        else:
            print(f"Processing {len(selected_schemas)} selected schemas: {', '.join(selected_schemas)}")
    
    # Generate the function Confluence files
    success = generate_function_confluence_files(json_file, output_dir, selected_schemas)
    
    if success:
        print("\nConfluence generation completed successfully!")
        print(f"Files generated in: {output_dir}")
        print("\nEach function now has:")
        print("  - JSON file with ADF content for Confluence import")
        print("  - JSON metadata file with function info")
        print("\nTo import into Confluence:")
        print("1. Use the ConfluencePageCreator.py interactive mode")
        print("2. Select option 3 or 4 to create pages from confluence_docs content")
        print("3. The ADF JSON content will be used for the page body")
        print("4. The metadata will be set as page properties")
        print("5. Page titles will be set automatically from the metadata")
    else:
        print("Confluence generation failed!")

if __name__ == "__main__":
    main()