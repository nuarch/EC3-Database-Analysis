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

def get_available_schemas(procedures):
    """Get list of all available schemas from the procedures data"""
    schemas = set()
    for proc in procedures:
        schema = proc['procedure_info']['schema']
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

def escape_xml(text):
    """Escape XML special characters"""
    if not text:
        return text
    return (text.replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#39;'))

def create_safe_filename(schema_name, procedure_name):
    """Create a safe filename from schema and procedure names"""
    # Remove or replace characters that are problematic in filenames
    safe_schema = re.sub(r'[<>:"/\\|?*]', '_', schema_name)
    safe_procedure = re.sub(r'[<>:"/\\|?*]', '_', procedure_name)
    return f"{safe_schema} - {safe_procedure}"

def create_procedure_metadata(proc):
    """Create metadata JSON for a stored procedure"""
    proc_info = proc['procedure_info']
    schema_name = proc_info['schema']
    procedure_name = proc_info['name']
    
    # Get complexity from ChatGPT analysis
    complexity = 'N/A'
    if 'chatgpt_explanation' in proc:
        analysis = proc['chatgpt_explanation']
        if 'complexity' in analysis and analysis['complexity']:
            complexity = analysis['complexity']
    
    metadata = {
        "stored_procedure_name": procedure_name,
        "schema_name": schema_name,
        "complexity": complexity,
        "generated_date": datetime.now().isoformat()
    }
    
    return metadata

def generate_procedure_page(proc):
    """Generate Confluence storage format content for a single stored procedure"""
    proc_info = proc['procedure_info']
    schema_name = proc_info['schema']
    procedure_name = proc_info['name']
    
    # Page title
    content = f'<h1>{escape_xml(schema_name)} - {escape_xml(procedure_name)}</h1>\n\n'
    
    # Schema info panel
    content += '<ac:structured-macro ac:name="info" ac:schema-version="1">\n'
    content += '<ac:parameter ac:name="title">Schema Information</ac:parameter>\n'
    content += '<ac:rich-text-body>\n'
    content += f'<p><strong>Schema:</strong> {escape_xml(schema_name)}</p>\n'
    content += f'<p><strong>Procedure:</strong> {escape_xml(procedure_name)}</p>\n'
    content += f'<p><strong>Generated:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>\n'
    content += '</ac:rich-text-body>\n'
    content += '</ac:structured-macro>\n\n'
    
    # ChatGPT Analysis
    if 'chatgpt_explanation' in proc:
        analysis = proc['chatgpt_explanation']
        
        # Complexity indicator
        if 'complexity' in analysis and analysis['complexity']:
            complexity = analysis['complexity']
            complexity_color = {
                'Low': 'Green',
                'Medium': 'Yellow', 
                'High': 'Red'
            }.get(complexity, 'Grey')
            
            content += '<ac:structured-macro ac:name="status" ac:schema-version="1">\n'
            content += f'<ac:parameter ac:name="colour">{complexity_color}</ac:parameter>\n'
            content += f'<ac:parameter ac:name="title">Complexity: {escape_xml(complexity)}</ac:parameter>\n'
            content += '</ac:structured-macro>\n\n'
        
        # Purpose
        if 'purpose' in analysis and analysis['purpose']:
            content += '<h2>Purpose</h2>\n'
            content += f'<p>{escape_xml(analysis["purpose"])}</p>\n\n'
        
        # Parameters
        if 'parameters' in analysis and analysis['parameters']:
            content += '<h2>Parameters</h2>\n'
            if isinstance(analysis['parameters'], list) and analysis['parameters']:
                content += '<table>\n<tbody>\n'
                content += '<tr><th>Parameter</th><th>Type</th><th>Description</th></tr>\n'
                for param in analysis['parameters']:
                    if isinstance(param, dict):
                        param_name = escape_xml(param.get('name', ''))
                        param_type = escape_xml(param.get('type', ''))
                        param_desc = escape_xml(param.get('description', ''))
                        content += f'<tr><td>{param_name}</td><td>{param_type}</td><td>{param_desc}</td></tr>\n'
                content += '</tbody>\n</table>\n\n'
            else:
                content += f'<p>{escape_xml(str(analysis["parameters"]))}</p>\n\n'
        
        # Returns
        if 'returns' in analysis and analysis['returns']:
            content += '<h2>Returns</h2>\n'
            content += f'<p>{escape_xml(analysis["returns"])}</p>\n\n'
        
        # Detailed explanation
        if 'explanation' in analysis and analysis['explanation']:
            content += '<h2>Detailed Analysis</h2>\n'
            explanation_text = analysis['explanation']
            
            # Convert markdown-style formatting to Confluence format
            explanation_text = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', explanation_text)
            explanation_text = re.sub(r'\*(.*?)\*', r'<em>\1</em>', explanation_text)
            explanation_text = re.sub(r'`(.*?)`', r'<code>\1</code>', explanation_text)
            
            # Split into paragraphs and wrap in <p> tags
            paragraphs = explanation_text.split('\n\n')
            for paragraph in paragraphs:
                if paragraph.strip():
                    content += f'<p>{escape_xml(paragraph.strip())}</p>\n'
            content += '\n'
    
    # Procedure Definition
    if 'definition' in proc_info and proc_info['definition']:
        content += '<h2>Procedure Definition</h2>\n'
        content += '<ac:structured-macro ac:name="code" ac:schema-version="1">\n'
        content += '<ac:parameter ac:name="language">sql</ac:parameter>\n'
        content += '<ac:parameter ac:name="collapse">false</ac:parameter>\n'
        content += '<ac:plain-text-body><![CDATA['
        content += proc_info['definition']
        content += ']]></ac:plain-text-body>\n'
        content += '</ac:structured-macro>\n\n'
    
    return content

def generate_procedure_confluence_files(json_file_path, output_dir="./confluence_docs/sps", selected_schemas=None):
    """Generate separate Confluence storage format files and metadata for each procedure"""
    
    # Load JSON data
    procedures = load_json_data(json_file_path)
    if not procedures:
        print("Failed to load JSON data")
        return False
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    # Filter procedures by selected schemas if specified
    if selected_schemas:
        filtered_procedures = []
        for proc in procedures:
            schema = proc['procedure_info']['schema']
            if schema in selected_schemas:
                filtered_procedures.append(proc)
        procedures = filtered_procedures
    
    if not procedures:
        print("No procedures to process")
        return False
    
    generated_files = []
    schema_counts = defaultdict(int)
    
    # Generate Confluence file and metadata for each procedure
    for proc in procedures:
        proc_info = proc['procedure_info']
        schema_name = proc_info['schema']
        procedure_name = proc_info['name']
        
        # Generate Confluence content
        confluence_content = generate_procedure_page(proc)
        
        # Create metadata
        metadata = create_procedure_metadata(proc)
        
        # Create filename base - keeping original capitalization
        filename_base = create_safe_filename(schema_name, procedure_name)
        xml_filename = f"{filename_base}.xml"
        json_filename = f"{filename_base}.json"
        
        xml_output_file = os.path.join(output_dir, xml_filename)
        json_output_file = os.path.join(output_dir, json_filename)
        
        # Count procedures per schema for summary
        schema_counts[schema_name] += 1
        
        # Write XML file
        try:
            with open(xml_output_file, 'w', encoding='utf-8') as file:
                file.write(confluence_content)
            print(f"Generated XML: {xml_filename}")
            generated_files.append(xml_output_file)
        except Exception as e:
            print(f"Error writing XML file {xml_output_file}: {e}")
            return False
        
        # Write JSON metadata file
        try:
            with open(json_output_file, 'w', encoding='utf-8') as file:
                json.dump(metadata, file, indent=2, ensure_ascii=False)
            print(f"Generated JSON: {json_filename}")
            generated_files.append(json_output_file)
        except Exception as e:
            print(f"Error writing JSON file {json_output_file}: {e}")
            return False
    
    # Print summary
    print(f"\nSuccessfully generated {len(generated_files)} files ({len(generated_files)//2} procedures):")
    print("\nProcedures by schema:")
    for schema, count in sorted(schema_counts.items()):
        print(f"  {schema}: {count} procedures")
    
    return True

def parse_command_line_args():
    """Parse command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate individual Confluence pages and metadata for stored procedures')
    parser.add_argument('--input', '-i', default='./export/stored_procedures_analysis_all_schemas.json',
                        help='Input JSON file path (default: ./export/stored_procedures_analysis_all_schemas.json)')
    parser.add_argument('--output', '-o', default='./confluence_docs/sps',
                        help='Output directory (default: ./confluence_docs/sps)')
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
    
    # Check if JSON file exists
    if not os.path.exists(json_file):
        print(f"JSON file not found: {json_file}")
        return
    
    # Load JSON data to get available schemas
    procedures = load_json_data(json_file)
    if not procedures:
        print("Failed to load JSON data")
        return
    
    available_schemas = get_available_schemas(procedures)
    
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
    
    # Generate the procedure Confluence files
    success = generate_procedure_confluence_files(json_file, output_dir, selected_schemas)
    
    if success:
        print("\nConfluence generation completed successfully!")
        print(f"Files generated in: {output_dir}")
        print("\nEach procedure now has:")
        print("  - XML file for Confluence import")
        print("  - JSON metadata file with procedure info")
        print("\nTo import into Confluence:")
        print("1. Copy the XML content from each file")
        print("2. In Confluence, create a new page")
        print("3. Switch to 'Storage Format' view")
        print("4. Paste the XML content")
        print("5. Switch back to normal editing view")
        print("6. The page title will be set automatically from the content")
    else:
        print("Confluence generation failed!")

if __name__ == "__main__":
    main()
