import json
import os
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

def escape_xml(text):
    """Escape XML special characters"""
    if not text:
        return text
    return (text.replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#39;'))

def create_anchor_id(text):
    """Create a Confluence-compatible anchor ID from text"""
    return text.lower().replace(' ', '-').replace('_', '-').replace('[', '').replace(']', '').replace('.', '')

def generate_schema_procedures(schema_name, procedures):
    """Generate Confluence storage format content for procedures in a specific schema"""
    content = f'<h1>{escape_xml(schema_name)} Schema - Stored Procedures</h1>\n\n'
    
    # Sort procedures within schema alphabetically
    sorted_procs = sorted(procedures, key=lambda x: x['procedure_info']['name'])
    
    # Add info panel with summary
    content += '<ac:structured-macro ac:name="info" ac:schema-version="1">\n'
    content += '<ac:parameter ac:name="title">Schema Summary</ac:parameter>\n'
    content += '<ac:rich-text-body>\n'
    content += f'<p>This page contains documentation for <strong>{len(sorted_procs)} stored procedures</strong> '
    content += f'in the <strong>{escape_xml(schema_name)}</strong> schema.</p>\n'
    content += '</ac:rich-text-body>\n'
    content += '</ac:structured-macro>\n\n'
    
    # Generate table of contents
    content += '<ac:structured-macro ac:name="toc" ac:schema-version="1">\n'
    content += '<ac:parameter ac:name="maxLevel">2</ac:parameter>\n'
    content += '</ac:structured-macro>\n\n'
    
    # Generate detailed sections for each procedure
    for proc in sorted_procs:
        proc_info = proc['procedure_info']
        name = proc_info['name']
        
        # Create anchor for linking
        anchor_id = create_anchor_id(name)
        content += f'<h2><ac:structured-macro ac:name="anchor" ac:schema-version="1">'
        content += f'<ac:parameter ac:name="name">{anchor_id}</ac:parameter>'
        content += f'</ac:structured-macro>{escape_xml(name)}</h2>\n\n'
        
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
                content += '<h3>Purpose</h3>\n'
                content += f'<p>{escape_xml(analysis["purpose"])}</p>\n\n'
            
            # Parameters
            if 'parameters' in analysis and analysis['parameters']:
                content += '<h3>Parameters</h3>\n'
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
                content += '<h3>Returns</h3>\n'
                content += f'<p>{escape_xml(analysis["returns"])}</p>\n\n'
            
            # Detailed explanation
            if 'explanation' in analysis and analysis['explanation']:
                content += '<h3>Detailed Analysis</h3>\n'
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
            content += '<h3>Procedure Definition</h3>\n'
            content += '<ac:structured-macro ac:name="code" ac:schema-version="1">\n'
            content += '<ac:parameter ac:name="language">sql</ac:parameter>\n'
            content += '<ac:parameter ac:name="collapse">false</ac:parameter>\n'
            content += '<ac:plain-text-body><![CDATA['
            content += proc_info['definition']
            content += ']]></ac:plain-text-body>\n'
            content += '</ac:structured-macro>\n\n'
        
        # Add separator between procedures
        content += '<hr />\n\n'
    
    return content

def generate_index_page(schema_groups):
    """Generate index page with links to all schema pages in Confluence format"""
    content = '<h1>Stored Procedures Analysis - Index</h1>\n\n'
    
    # Info panel with generation date
    content += '<ac:structured-macro ac:name="info" ac:schema-version="1">\n'
    content += '<ac:parameter ac:name="title">Generated Information</ac:parameter>\n'
    content += '<ac:rich-text-body>\n'
    content += f'<p>Generated on: <strong>{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</strong></p>\n'
    content += '</ac:rich-text-body>\n'
    content += '</ac:structured-macro>\n\n'
    
    # Calculate complexity statistics
    complexity_counts = {'Low': 0, 'Medium': 0, 'High': 0, 'N/A': 0}
    total_procedures = 0
    
    for schema_procedures in schema_groups.values():
        total_procedures += len(schema_procedures)
        for proc in schema_procedures:
            complexity = proc.get('chatgpt_explanation', {}).get('complexity', 'N/A')
            if complexity in complexity_counts:
                complexity_counts[complexity] += 1
            else:
                complexity_counts['N/A'] += 1
    
    # Summary statistics in an info panel
    content += '<ac:structured-macro ac:name="panel" ac:schema-version="1">\n'
    content += '<ac:parameter ac:name="bgColor">#deebff</ac:parameter>\n'
    content += '<ac:parameter ac:name="title">Summary Statistics</ac:parameter>\n'
    content += '<ac:rich-text-body>\n'
    content += f'<p><strong>Total Schemas:</strong> {len(schema_groups)}</p>\n'
    content += f'<p><strong>Total Procedures:</strong> {total_procedures}</p>\n'
    content += '</ac:rich-text-body>\n'
    content += '</ac:structured-macro>\n\n'
    
    # Complexity breakdown
    content += '<h2>Complexity Distribution</h2>\n'
    content += '<table>\n<tbody>\n'
    content += '<tr><th>Complexity Level</th><th>Count</th><th>Percentage</th></tr>\n'
    
    for complexity in ['Low', 'Medium', 'High', 'N/A']:
        count = complexity_counts[complexity]
        percentage = (count / total_procedures * 100) if total_procedures > 0 else 0
        content += f'<tr><td>{escape_xml(complexity)}</td><td>{count}</td><td>{percentage:.1f}%</td></tr>\n'
    
    content += '</tbody>\n</table>\n\n'
    
    # Schema links
    content += '<h2>Schemas</h2>\n'
    content += '<table>\n<tbody>\n'
    content += '<tr><th>Schema</th><th>Total Procedures</th><th>Complexity Breakdown</th><th>Link</th></tr>\n'
    
    for schema in sorted(schema_groups.keys()):
        procedure_count = len(schema_groups[schema])
        schema_page = f"{schema.lower().replace(' ', '_')}_procedures"
        
        # Calculate complexity for this schema
        schema_complexity = {'Low': 0, 'Medium': 0, 'High': 0, 'N/A': 0}
        for proc in schema_groups[schema]:
            complexity = proc.get('chatgpt_explanation', {}).get('complexity', 'N/A')
            if complexity in schema_complexity:
                schema_complexity[complexity] += 1
            else:
                schema_complexity['N/A'] += 1
        
        complexity_summary = f"L:{schema_complexity['Low']}, M:{schema_complexity['Medium']}, H:{schema_complexity['High']}"
        if schema_complexity['N/A'] > 0:
            complexity_summary += f", N/A:{schema_complexity['N/A']}"
        
        content += f'<tr>\n'
        content += f'<td><strong>{escape_xml(schema)}</strong></td>\n'
        content += f'<td>{procedure_count}</td>\n'
        content += f'<td>{escape_xml(complexity_summary)}</td>\n'
        content += f'<td><ac:link><ri:page ri:content-title="{escape_xml(schema_page)}" /></ac:link></td>\n'
        content += f'</tr>\n'
    
    content += '</tbody>\n</table>\n\n'
    
    return content

def generate_schema_confluence_files(json_file_path, output_dir="./confluence_docs"):
    """Generate separate Confluence storage format files for each schema"""
    
    # Load JSON data
    procedures = load_json_data(json_file_path)
    if not procedures:
        print("Failed to load JSON data")
        return False
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    # Group procedures by schema
    schema_groups = defaultdict(list)
    for proc in procedures:
        schema = proc['procedure_info']['schema']
        schema_groups[schema].append(proc)
    
    generated_files = []
    
    # Generate Confluence file for each schema
    for schema, schema_procedures in schema_groups.items():
        # Generate Confluence content
        confluence_content = generate_schema_procedures(schema, schema_procedures)
        
        # Create filename
        schema_filename = f"{schema.lower().replace(' ', '_')}_procedures.xml"
        output_file = os.path.join(output_dir, schema_filename)
        
        # Write to file
        try:
            with open(output_file, 'w', encoding='utf-8') as file:
                file.write(confluence_content)
            print(f"Generated: {output_file} ({len(schema_procedures)} procedures)")
            generated_files.append(output_file)
        except Exception as e:
            print(f"Error writing file {output_file}: {e}")
            return False
    
    # Generate index page
    index_content = generate_index_page(schema_groups)
    index_file = os.path.join(output_dir, "index.xml")
    
    try:
        with open(index_file, 'w', encoding='utf-8') as file:
            file.write(index_content)
        print(f"Generated index: {index_file}")
        generated_files.append(index_file)
    except Exception as e:
        print(f"Error writing index file: {e}")
        return False
    
    print(f"\nSuccessfully generated {len(generated_files)} Confluence files:")
    for file_path in generated_files:
        print(f"  - {file_path}")
    
    return True

def main():
    """Main function"""
    # File paths
    json_file = "./export/stored_procedures_analysis_all_schemas.json"
    output_dir = "./confluence_docs"
    
    # Check if JSON file exists
    if not os.path.exists(json_file):
        print(f"JSON file not found: {json_file}")
        return
    
    # Generate the schema Confluence files
    success = generate_schema_confluence_files(json_file, output_dir)
    
    if success:
        print("\nConfluence generation completed successfully!")
        print(f"Files generated in: {output_dir}")
        print("\nTo import into Confluence:")
        print("1. Copy the XML content from each file")
        print("2. In Confluence, create a new page")
        print("3. Switch to 'Storage Format' view")
        print("4. Paste the XML content")
        print("5. Switch back to normal editing view")
    else:
        print("Confluence generation failed!")

if __name__ == "__main__":
    main()