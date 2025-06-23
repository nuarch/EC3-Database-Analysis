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

def generate_schema_procedures(schema_name, procedures):
    """Generate markdown content for procedures in a specific schema"""
    md_content = f"# {schema_name} Schema - Stored Procedures\n\n"
    
    # Sort procedures within schema alphabetically
    sorted_procs = sorted(procedures, key=lambda x: x['procedure_info']['name'])
    
    # Generate table of contents
    md_content += "## Table of Contents\n\n"
    for proc in sorted_procs:
        name = proc['procedure_info']['name']
        anchor = create_anchor_link(name)
        md_content += f"- [{name}]\n"
    md_content += "\n"
    
    # Generate detailed sections for each procedure
    for proc in sorted_procs:
        proc_info = proc['procedure_info']
        name = proc_info['name']
        
        # Create anchor for linking
        anchor = create_anchor_link(name)
        md_content += f"## {name}\n\n" # {{#{anchor}}}\n\n"
        
        # ChatGPT Analysis
        if 'chatgpt_explanation' in proc:
            analysis = proc['chatgpt_explanation']
            
            # Detailed explanation - clean it first
            if 'explanation' in analysis and analysis['explanation']:
                explanation_text = analysis['explanation']
                
                # Insert anchor into the first heading within the explanation
                lines = explanation_text.split('\n')
                anchor_inserted = False
                
                for i, line in enumerate(lines):
                    # Check if this is a heading (starts with #)
                    if line.strip().startswith('### Analysis of Stored Procedure: ') and not anchor_inserted:
                        # Extract the heading text without the # symbols
                        heading_level = len(line) - len(line.lstrip('#'))
                        heading_text = line.lstrip('### Analysis of Stored Procedure: ').strip('`')
                        
                        # Reconstruct the heading with anchor
                        lines[i] = '#' * heading_level + f" {heading_text}"
                        anchor_inserted = True
                        break
                
                # Join the lines back together
                explanation_text = '\n'.join(lines)
                md_content += explanation_text + "\n\n"
        
        # Procedure Definition
        if 'definition' in proc_info and proc_info['definition']:
            md_content += "**Procedure Definition:**\n\n"
            md_content += "```sql\n"
            md_content += proc_info['definition']
            md_content += "\n```\n\n"
        
        md_content += "---\n\n"
    
    return md_content

def generate_index_page(schema_groups):
    """Generate index page with links to all schema pages"""
    md_content = "# Stored Procedures Analysis - Index\n\n"
    md_content += f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
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
    
    # Summary statistics
    md_content += f"**Total Schemas:** {len(schema_groups)}\n\n"
    md_content += f"**Total Procedures:** {total_procedures}\n\n"
    
    # Complexity breakdown
    md_content += "## Complexity Distribution\n\n"
    md_content += "| Complexity Level | Count | Percentage |\n"
    md_content += "|------------------|-------|------------|\n"
    
    for complexity in ['Low', 'Medium', 'High', 'N/A']:
        count = complexity_counts[complexity]
        percentage = (count / total_procedures * 100) if total_procedures > 0 else 0
        md_content += f"| {complexity} | {count} | {percentage:.1f}% |\n"
    
    md_content += "\n"
    
    # Schema links with complexity breakdown
    md_content += "## Schemas\n\n"
    for schema in sorted(schema_groups.keys()):
        procedure_count = len(schema_groups[schema])
        schema_file = f"{schema.lower().replace(' ', '_')}_procedures.md"
        
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
        
        md_content += f"- [{schema}]({schema_file}) ({procedure_count} procedures - {complexity_summary})\n"
    
    md_content += "\n"
    
    # Detailed summary table
    md_content += "## Detailed Summary Table\n\n"
    md_content += "| Schema | Total | Low | Medium | High | N/A | File |\n"
    md_content += "|--------|-------|-----|--------|------|-----|------|\n"
    
    for schema in sorted(schema_groups.keys()):
        procedure_count = len(schema_groups[schema])
        schema_file = f"{schema.lower().replace(' ', '_')}_procedures.md"
        
        # Calculate complexity for this schema
        schema_complexity = {'Low': 0, 'Medium': 0, 'High': 0, 'N/A': 0}
        for proc in schema_groups[schema]:
            complexity = proc.get('chatgpt_explanation', {}).get('complexity', 'N/A')
            if complexity in schema_complexity:
                schema_complexity[complexity] += 1
            else:
                schema_complexity['N/A'] += 1
        
        md_content += f"| {schema} | {procedure_count} | {schema_complexity['Low']} | {schema_complexity['Medium']} | {schema_complexity['High']} | {schema_complexity['N/A']} | [{schema_file}]({schema_file}) |\n"
    
    return md_content

def generate_schema_markdown_files(json_file_path, output_dir="./docs"):
    """Generate separate markdown files for each schema"""
    
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
    
    # Generate markdown file for each schema
    for schema, schema_procedures in schema_groups.items():
        # Generate markdown content
        md_content = generate_schema_procedures(schema, schema_procedures)
        
        # Create filename
        schema_filename = f"{schema.lower().replace(' ', '_')}_procedures.md"
        output_file = os.path.join(output_dir, schema_filename)
        
        # Write to file
        try:
            with open(output_file, 'w', encoding='utf-8') as file:
                file.write(md_content)
            print(f"Generated: {output_file} ({len(schema_procedures)} procedures)")
            generated_files.append(output_file)
        except Exception as e:
            print(f"Error writing file {output_file}: {e}")
            return False
    
    # Generate index page
    index_content = generate_index_page(schema_groups)
    index_file = os.path.join(output_dir, "index.md")
    
    try:
        with open(index_file, 'w', encoding='utf-8') as file:
            file.write(index_content)
        print(f"Generated index: {index_file}")
        generated_files.append(index_file)
    except Exception as e:
        print(f"Error writing index file: {e}")
        return False
    
    print(f"\nSuccessfully generated {len(generated_files)} files:")
    for file_path in generated_files:
        print(f"  - {file_path}")
    
    return True

def main():
    """Main function"""
    # File paths
    json_file = "./export/stored_procedures_analysis_all_schemas.json"
    output_dir = "./docs"
    
    # Check if JSON file exists
    if not os.path.exists(json_file):
        print(f"JSON file not found: {json_file}")
        return
    
    # Generate the schema markdown files
    success = generate_schema_markdown_files(json_file, output_dir)
    
    if success:
        print("\nMarkdown generation completed successfully!")
        print(f"Files generated in: {output_dir}")
    else:
        print("Markdown generation failed!")

if __name__ == "__main__":
    main()
