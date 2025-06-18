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

def create_anchor_link(text):
    """Create an anchor link from text"""
    return text.lower().replace(' ', '-').replace('_', '-').replace('[', '').replace(']', '').replace('.', '')

def clean_explanation_text(explanation):
    """Remove headings from the explanation text"""
    if not explanation:
        return explanation
    
    # Remove lines that start with ### (including any whitespace before)
    lines = explanation.split('\n')
    cleaned_lines = []
    
    for line in lines:
        # Skip lines that start with ### (after stripping whitespace)
        if not line.strip().startswith('### Analysis of Stored Procedure'):
            cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines).strip()

def generate_summary_table(procedures):
    """Generate summary table with procedure name, schema, and complexity"""
    md_content = "# Summary Table\n\n"
    md_content += "| Stored Procedure | Schema | Complexity |\n"
    md_content += "|------------------|--------|-----------|\n"
    
    # Sort procedures by name for consistent ordering
    sorted_procedures = sorted(procedures, key=lambda x: x['procedure_info']['name'])
    
    for proc in sorted_procedures:
        name = proc['procedure_info']['name']
        schema = proc['procedure_info']['schema']
        complexity = proc.get('chatgpt_explanation', {}).get('complexity', 'N/A')
        
        # Create link to detailed section
        anchor = create_anchor_link(f"{schema}-{name}")
        name_link = f"[{name}](#{anchor})"
        
        md_content += f"| {name_link} | {schema} | {complexity} |\n"
    
    md_content += "\n"
    return md_content

def generate_procedures_by_schema(procedures):
    """Generate procedures grouped by schema"""
    # Group procedures by schema
    schema_groups = defaultdict(list)
    for proc in procedures:
        schema = proc['procedure_info']['schema']
        schema_groups[schema].append(proc)
    
    md_content = "# Stored Procedures by Schema\n\n"
    
    # Sort schemas alphabetically
    for schema in sorted(schema_groups.keys()):
        md_content += f"## {schema}\n\n"
        
        # Sort procedures within schema alphabetically
        sorted_procs = sorted(schema_groups[schema], key=lambda x: x['procedure_info']['name'])
        
        for proc in sorted_procs:
            proc_info = proc['procedure_info']
            name = proc_info['name']
            
            # Create anchor for linking
            anchor = create_anchor_link(f"{schema}-{name}")
            md_content += f"#### {name} {{#{anchor}}}\n\n"
            
            # Parameters
            if 'parameters' in proc and proc['parameters']:
                md_content += "**Parameters:**\n"
                for param in proc['parameters']:
                    md_content += f"- **{param['name']}**\n"
                    md_content += f"  - Data Type: {param['data_type']}\n"
                    md_content += f"  - Mode: {param['mode']}\n"
                    md_content += f"  - Precision: {param.get('precision', 'N/A')}\n"
                    md_content += f"  - Scale: {param.get('scale', 'N/A')}\n"
                    if param.get('max_length'):
                        md_content += f"  - Max Length: {param['max_length']}\n"
                md_content += "\n"
            
            # ChatGPT Analysis
            if 'chatgpt_explanation' in proc:
                analysis = proc['chatgpt_explanation']
                md_content += f"**Complexity:** {analysis.get('complexity', 'N/A')}\n"

                md_content += "\n"
                
                # Detailed explanation - clean it first
                if 'explanation' in analysis and analysis['explanation']:
                    cleaned_explanation = clean_explanation_text(analysis['explanation'])
                    if cleaned_explanation:
                        md_content += "**Detailed Explanation:**\n\n"
                        md_content += cleaned_explanation + "\n\n"
            
            # Procedure Definition
            if 'definition' in proc_info and proc_info['definition']:
                md_content += "**Procedure Definition:**\n\n"
                md_content += "```sql\n"
                md_content += proc_info['definition']
                md_content += "\n```\n\n"
            
            md_content += "---\n\n"
    
    return md_content

def generate_markdown_report(json_file_path, output_file_path):
    """Generate markdown report from JSON data"""
    
    # Load JSON data
    procedures = load_json_data(json_file_path)
    if not procedures:
        print("Failed to load JSON data")
        return False
    
    # Start building markdown content
    md_content = "# Stored Procedures Analysis Report\n\n"
    
    # Generate summary table
    md_content += generate_summary_table(procedures)
    
    # Generate procedures by schema
    md_content += generate_procedures_by_schema(procedures)
    
    # Write to file
    try:
        with open(output_file_path, 'w', encoding='utf-8') as file:
            file.write(md_content)
        print(f"Markdown report successfully generated: {output_file_path}")
        return True
    except Exception as e:
        print(f"Error writing markdown file: {e}")
        return False

def main():
    """Main function"""
    # File paths
    json_file = "./backup/stored_procedures_analysis_ActualBill.json"
    output_file = "stored_procedures.md"
    
    # Check if JSON file exists
    if not os.path.exists(json_file):
        print(f"JSON file not found: {json_file}")
        return
    
    # Generate the markdown report
    success = generate_markdown_report(json_file, output_file)
    
    if success:
        print("Report generation completed successfully!")
    else:
        print("Report generation failed!")

if __name__ == "__main__":
    main()
