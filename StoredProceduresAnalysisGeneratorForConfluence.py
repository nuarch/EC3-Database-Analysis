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

def convert_markdown_to_html(markdown_text):
    """
    Convert markdown text to HTML.

    Args:
        markdown_text (str): The markdown text to convert

    Returns:
        str: The converted HTML text
    """
    html = markdown_text

    # Convert headers (### -> h3, #### -> h4, etc.)
    html = re.sub(r'^### (.*?)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
    html = re.sub(r'^#### (.*?)$', r'<h4>\1</h4>', html, flags=re.MULTILINE)
    html = re.sub(r'^##### (.*?)$', r'<h5>\1</h5>', html, flags=re.MULTILINE)
    html = re.sub(r'^###### (.*?)$', r'<h6>\1</h6>', html, flags=re.MULTILINE)
    html = re.sub(r'^## (.*?)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
    html = re.sub(r'^# (.*?)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)

    # Convert bold text (**text** -> <strong>text</strong>)
    html = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', html)

    # Convert italic text (*text* -> <em>text</em>)
    html = re.sub(r'\*(.*?)\*', r'<em>\1</em>', html)

    # Convert inline code (`code` -> <code>code</code>)
    html = re.sub(r'`([^`]+)`', r'<code>\1</code>', html)

    # Convert code blocks (```language\ncode\n``` -> <pre><code>code</code></pre>)
    html = re.sub(r'```[\w]*\n(.*?)\n```', r'<pre><code>\1</code></pre>', html, flags=re.DOTALL)

    # Convert bullet points and nested ordered lists
    lines = html.split('\n')
    result_lines = []
    in_unordered_list = False
    in_ordered_list = False
    in_nested_ordered_list = False

    for line in lines:
        stripped_line = line.strip()
        original_line = line

        # Check if this is a bullet point
        if stripped_line.startswith('- '):
            # Close any nested ordered list
            if in_nested_ordered_list:
                result_lines.append('    </ol>')
                in_nested_ordered_list = False
            # Close standalone ordered list if we were in one
            if in_ordered_list and not in_unordered_list:
                result_lines.append('</ol>')
                in_ordered_list = False

            if not in_unordered_list:
                result_lines.append('<ul>')
                in_unordered_list = True
            # Remove the '- ' and wrap in <li>
            list_item = stripped_line[2:]
            result_lines.append(f'  <li>{list_item}</li>')

        # Check if this is an indented numbered list item (nested within bullet points)
        elif re.match(r'^\s+\d+\.\s+', original_line):
            if not in_nested_ordered_list:
                result_lines.append('    <ol>')
                in_nested_ordered_list = True
            # Remove the indentation, number and dot, wrap in <li>
            list_item = re.sub(r'^\s+\d+\.\s+', '', original_line)
            result_lines.append(f'      <li>{list_item}</li>')

        # Check if this is a top-level numbered list item
        elif re.match(r'^\d+\.\s+', stripped_line):
            # Close nested ordered list if we were in one
            if in_nested_ordered_list:
                result_lines.append('    </ol>')
                in_nested_ordered_list = False
            # Close unordered list if we were in one
            if in_unordered_list:
                result_lines.append('</ul>')
                in_unordered_list = False

            if not in_ordered_list:
                result_lines.append('<ol>')
                in_ordered_list = True
            # Remove the number and dot, wrap in <li>
            list_item = re.sub(r'^\d+\.\s+', '', stripped_line)
            result_lines.append(f'  <li>{list_item}</li>')

        else:
            # If we were in any list and this line is not a list item, close the lists
            if in_nested_ordered_list:
                result_lines.append('    </ol>')
                in_nested_ordered_list = False
            if in_unordered_list:
                result_lines.append('</ul>')
                in_unordered_list = False
            if in_ordered_list:
                result_lines.append('</ol>')
                in_ordered_list = False
            result_lines.append(line)

    # Close any remaining open lists
    if in_nested_ordered_list:
        result_lines.append('    </ol>')
    if in_unordered_list:
        result_lines.append('</ul>')
    if in_ordered_list:
        result_lines.append('</ol>')

    html = '\n'.join(result_lines)

    # Convert line breaks to paragraphs
    # Split by double line breaks to identify paragraphs
    paragraphs = html.split('\n\n')
    html_paragraphs = []

    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        if paragraph:
            # Don't wrap headers, lists, code blocks, or already wrapped HTML in <p> tags
            if not (paragraph.startswith('<h') or paragraph.startswith('<ul>') or
                    paragraph.startswith('<ol>') or paragraph.startswith('<pre>') or
                    paragraph.startswith('<li>') or paragraph.startswith('</ul>') or
                    paragraph.startswith('</ol>') or paragraph.endswith('</h1>') or
                    paragraph.endswith('</h2>') or paragraph.endswith('</h3>') or
                    paragraph.endswith('</h4>') or paragraph.endswith('</h5>') or
                    paragraph.endswith('</h6>')):
                # Replace single line breaks with <br> within paragraphs
                paragraph = paragraph.replace('\n', '<br>\n')
                paragraph = f'<p>{paragraph}</p>'
            html_paragraphs.append(paragraph)

    html = '\n\n'.join(html_paragraphs)

    return html

def convert_html_to_confluence_storage_format(html_content):
    """
    Convert HTML content to Confluence Storage Format.

    Args:
        html_content (str): The HTML content to convert

    Returns:
        str: The Confluence Storage Format XML
    """
    confluence_xml = html_content

    # Convert headers to Confluence format
    confluence_xml = re.sub(r'<h1>(.*?)</h1>', r'<h1>\1</h1>', confluence_xml)
    confluence_xml = re.sub(r'<h2>(.*?)</h2>', r'<h2>\1</h2>', confluence_xml)
    confluence_xml = re.sub(r'<h3>(.*?)</h3>', r'<h3>\1</h3>', confluence_xml)
    confluence_xml = re.sub(r'<h4>(.*?)</h4>', r'<h4>\1</h4>', confluence_xml)
    confluence_xml = re.sub(r'<h5>(.*?)</h5>', r'<h5>\1</h5>', confluence_xml)
    confluence_xml = re.sub(r'<h6>(.*?)</h6>', r'<h6>\1</h6>', confluence_xml)

    # Convert paragraphs to Confluence format
    confluence_xml = re.sub(r'<p>(.*?)</p>', r'<p>\1</p>', confluence_xml, flags=re.DOTALL)

    # Convert line breaks
    confluence_xml = re.sub(r'<br\s*/?>', r'<br/>', confluence_xml)

    # Convert strong/bold to Confluence format
    confluence_xml = re.sub(r'<strong>(.*?)</strong>', r'<strong>\1</strong>', confluence_xml)

    # Convert emphasis/italic to Confluence format
    confluence_xml = re.sub(r'<em>(.*?)</em>', r'<em>\1</em>', confluence_xml)

    # Convert inline code to Confluence code macro
    confluence_xml = re.sub(r'<code>(.*?)</code>', r'<ac:structured-macro ac:name="code" ac:schema-version="1"><ac:parameter ac:name="language">text</ac:parameter><ac:plain-text-body><![CDATA[\1]]></ac:plain-text-body></ac:structured-macro>', confluence_xml)

    # Convert code blocks to Confluence code macro
    confluence_xml = re.sub(
        r'<pre><code>(.*?)</code></pre>',
        r'<ac:structured-macro ac:name="code" ac:schema-version="1"><ac:parameter ac:name="language">text</ac:parameter><ac:parameter ac:name="theme">Confluence</ac:parameter><ac:plain-text-body><![CDATA[\1]]></ac:plain-text-body></ac:structured-macro>',
        confluence_xml,
        flags=re.DOTALL
    )

    # Convert unordered lists - Confluence uses the same ul/li structure
    # No changes needed for basic ul/li

    # Convert ordered lists - Confluence uses the same ol/li structure
    # No changes needed for basic ol/li

    # Clean up any remaining HTML tags that don't have direct Confluence equivalents
    # Remove any standalone <br> that might interfere with formatting
    confluence_xml = re.sub(r'<br/>\s*</p>', '</p>', confluence_xml)
    confluence_xml = re.sub(r'<p>\s*<br/>', '<p>', confluence_xml)

    # Wrap in Confluence storage format structure
    confluence_storage_format = f'''<ac:confluence xmlns:ac="http://www.atlassian.com/schema/confluence/4/ac/" xmlns:ri="http://www.atlassian.com/schema/confluence/4/ri/">
{confluence_xml}
</ac:confluence>'''

    return confluence_storage_format

def format_confluence_content(text) :
    """
    Format Markdown text content for Confluence storage format.

    Args:
        text (str): The text content to format

    Returns:
        str: The formatted Confluence storage format content
    """
    # Convert Markdown to HTML
    html_content = convert_markdown_to_html(text)

    # Convert HTML to Confluence storage format
    confluence_content = convert_html_to_confluence_storage_format(html_content)

    return confluence_content

def create_procedure_metadata(proc):
    """Create metadata JSON for a stored procedure"""
    proc_info = proc['procedure_info']
    analysis = proc.get('analysis', {}) or proc.get('chatgpt_explanation', {})
    
    schema_name = proc_info['schema']
    procedure_name = proc_info['name']
    
    # Get complexity from analysis
    complexity = 'N/A'
    if isinstance(analysis, dict):
        if 'complexity' in analysis and analysis['complexity']:
            complexity = analysis['complexity']
        elif 'complexity_score' in analysis and analysis['complexity_score']:
            complexity = analysis['complexity_score']
    
    # Basic metadata
    metadata = {
        "title": f"{schema_name} - {procedure_name}",
        "stored_procedure_name": procedure_name,
        "schema": schema_name,
        "type": "Stored Procedure",
        "complexity": complexity,
        "generated_date": datetime.now().isoformat(),
        "description": f"Analysis and documentation for stored procedure {procedure_name} in schema {schema_name}"
    }
    
    # Add additional metadata from procedure info
    if proc_info.get('created_date'):
        metadata['created_date'] = proc_info['created_date']
    
    if proc_info.get('modified_date'):
        metadata['modified_date'] = proc_info['modified_date']
    
    if proc_info.get('description'):
        metadata['description'] = proc_info['description']
    
    # Add analysis metadata if available
    if isinstance(analysis, dict):
        if analysis.get('purpose'):
            metadata['purpose'] = analysis['purpose']
        
        if analysis.get('returns'):
            metadata['returns'] = analysis['returns']
        
        if analysis.get('business_logic'):
            metadata['business_logic_summary'] = analysis['business_logic'][:200] + "..." if len(str(analysis['business_logic'])) > 200 else analysis['business_logic']
    
    return metadata

def generate_procedure_page(proc):
    """Generate Confluence storage format content for a single stored procedure"""
    proc_info = proc['procedure_info']
    analysis = proc.get('analysis', {}) or proc.get('chatgpt_explanation', {})
    
    schema_name = proc_info['schema']
    procedure_name = proc_info['name']
    
    content = ''
    
    # Page title
    content += f'<h1>{escape_xml(schema_name)} - {escape_xml(procedure_name)}</h1>\n\n'

    # Analysis sections
    if isinstance(analysis, dict):
        # Detailed explanation
        if analysis.get('explanation'):
            content += '<h2>Detailed Analysis</h2>\n'

            # Remove any existing header that might conflict
            text = analysis['explanation']
            text = re.sub(r'###\s+Analysis\s+of\s+Stored\s+Procedure:\s*(\w+)\s*\n\s*\n', '', text, flags=re.MULTILINE)

            # Promote all headings up one level (remove one # from each heading)
            # Process from most specific to least specific to avoid conflicts
            text = re.sub(r'^## (.*?)$', r'# \1', text, flags=re.MULTILINE)          # h2 -> h1
            text = re.sub(r'^### (.*?)$', r'## \1', text, flags=re.MULTILINE)        # h3 -> h2
            text = re.sub(r'^#### (.*?)$', r'### \1', text, flags=re.MULTILINE)      # h4 -> h3
            text = re.sub(r'^##### (.*?)$', r'#### \1', text, flags=re.MULTILINE)    # h5 -> h4
            text = re.sub(r'^###### (.*?)$', r'##### \1', text, flags=re.MULTILINE)  # h6 -> h5

            formatted_explanation = format_confluence_content(text)
            content += formatted_explanation + '\n\n'
    
    # Procedure Definition/Source Code
    definition_field = proc_info.get('definition') or proc_info.get('source_code')
    if definition_field:
        content += '<h2>Procedure Definition</h2>\n'
        content += '<ac:structured-macro ac:name="code" ac:schema-version="1">\n'
        content += '<ac:parameter ac:name="language">sql</ac:parameter>\n'
        content += '<ac:parameter ac:name="collapse">false</ac:parameter>\n'
        content += '<ac:plain-text-body><![CDATA['
        content += definition_field
        content += ']]></ac:plain-text-body>\n'
        content += '</ac:structured-macro>\n\n'
    
    # Analysis Metadata
    if isinstance(analysis, dict) and analysis.get('metadata'):
        metadata = analysis['metadata']
        content += '<h2>Analysis Metadata</h2>\n'
        content += '<table>\n<tbody>\n'
        
        if metadata.get('analyzed_date'):
            content += f'<tr><td><strong>Analysis Date</strong></td><td>{escape_xml(metadata["analyzed_date"])}</td></tr>\n'
        
        if metadata.get('analysis_version'):
            content += f'<tr><td><strong>Analysis Version</strong></td><td>{escape_xml(metadata["analysis_version"])}</td></tr>\n'
        
        if metadata.get('tokens_used'):
            content += f'<tr><td><strong>Tokens Used</strong></td><td>{escape_xml(str(metadata["tokens_used"]))}</td></tr>\n'
        
        content += '</tbody>\n</table>\n'
    
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
    
    # Check if a JSON file exists
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
        print("1. Use the ConfluencePageCreator.py interactive mode")
        print("2. Select option 3 or 4 to create pages from confluence_docs content")
        print("3. The XML content will be used for the page body")
        print("4. The JSON metadata will be set as page properties")
        print("5. Page titles will be set automatically from the metadata")
    else:
        print("Confluence generation failed!")

if __name__ == "__main__":
    main()
