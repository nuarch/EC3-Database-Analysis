#!/usr/bin/env python3
"""
Markdown to HTML Converter
Converts ChatGPT markdown explanations to HTML format.
"""

import re
import sys
import argparse
from pathlib import Path


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


def create_full_html_document(content, title="Converted Document"):
  """
  Wrap the converted HTML content in a complete HTML document structure.

  Args:
      content (str): The HTML content
      title (str): The document title

  Returns:
      str: Complete HTML document
  """
  return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            color: #333;
        }}
        h1, h2, h3, h4, h5, h6 {{
            color: #2c3e50;
            margin-top: 1.5em;
            margin-bottom: 0.5em;
        }}
        code {{
            background-color: #f4f4f4;
            padding: 2px 4px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
        }}
        pre {{
            background-color: #f4f4f4;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
        }}
        pre code {{
            background-color: transparent;
            padding: 0;
        }}
        ul, ol {{
            padding-left: 20px;
        }}
        li {{
            margin: 0.5em 0;
        }}
        strong {{
            color: #2c3e50;
        }}
    </style>
</head>
<body>
{content}
</body>
</html>"""


def main():
  """Main function demonstrating the markdown to HTML and Confluence conversion with example text."""

  # Example markdown text (similar to ChatGPT explanations)
  example_markdown = """### Analysis of Stored Procedure: usp_ArchiveConEdisonRawDataFile

#### 1. Clear Explanation of Functionality
The stored procedure `usp_ArchiveConEdisonRawDataFile` is designed to archive raw data from Con Edison into historical tables. It performs the following operations:
- Inserts data from the `ConEd.UploadConEdisonAccount` table into the `ActualBill.HistoryUploadLegacyConEdisonRawAccountData` table.
- Inserts data from the `ConEd.UploadConEdisonMeter` table into the `ActualBill.HistoryUploadLegacyConEdisonRawMeterData` table.
- Inserts data from the `ConEd.UploadConEdisonCancellation` table into the `ActualBill.HistoryUploadLegacyConEdisonRawCancellationData` table.

Each of these operations involves copying all columns from the source tables to the corresponding destination tables.

#### 2. Complexity Level
- **Complexity Level: Low**
  - The procedure consists of straightforward `INSERT INTO ... SELECT` statements without any conditional logic, loops, or complex transformations.

#### 3. Input Parameters and Their Purposes
- **@Status AS INT OUTPUT**: This parameter is intended to return a status code indicating the success or failure of the procedure. However, the current implementation does not set or modify this parameter, which means it does not serve its intended purpose.

#### 4. Business Logic and Workflow
- **Business Logic**: The procedure is used to archive data from current tables into historical tables. This is typically done to maintain a record of past data for auditing, reporting, or backup purposes.
- **Workflow**:
  1. Data from `ConEd.UploadConEdisonAccount` is copied to `ActualBill.HistoryUploadLegacyConEdisonRawAccountData`.
  2. Data from `ConEd.UploadConEdisonMeter` is copied to `ActualBill.HistoryUploadLegacyConEdisonRawMeterData`.
  3. Data from `ConEd.UploadConEdisonCancellation` is copied to `ActualBill.HistoryUploadLegacyConEdisonRawCancellationData`.

#### 5. Performance Considerations
- **Bulk Inserts**: The procedure performs bulk inserts, which can be efficient but may also lock tables and impact performance if the tables are large or heavily accessed.
- **Indexing**: Ensure that the destination tables are properly indexed to handle large volumes of data efficiently.
- **Transaction Management**: The procedure does not explicitly manage transactions. If one insert fails, the others will still execute, potentially leading to inconsistent data states.

#### 6. Potential Issues or Risks
- **Lack of Error Handling**: The procedure does not include error handling or transaction management, which could lead to partial data archiving if an error occurs.
- **Unused Output Parameter**: The `@Status` output parameter is not utilized, which could lead to confusion or misinterpretation of the procedure's success.
- **Data Consistency**: Without transaction management, there's a risk of data inconsistency if one of the insert operations fails.
- **Scalability**: As data grows, the performance of bulk inserts may degrade, and additional strategies such as partitioning or batching may be required.

#### Recommendations
- Implement error handling and transaction management to ensure data consistency and reliability.
- Utilize the `@Status` parameter to provide meaningful feedback on the procedure's execution.
- Consider indexing strategies and performance tuning for large datasets.
- Evaluate the need for archiving frequency and adjust the procedure to handle data growth effectively."""

  print("Converting example markdown to HTML and Confluence Storage Format...")
  print("=" * 70)

  # Convert markdown to HTML
  html_content = convert_markdown_to_html(example_markdown)

  # Convert HTML to Confluence Storage Format
  confluence_content = convert_html_to_confluence_storage_format(html_content)

  # Create full HTML document
  full_html = create_full_html_document(html_content, "Stored Procedure Analysis")

  # Save HTML file
  html_output_path = Path("converted_example.html")
  try:
    with open(html_output_path, 'w', encoding='utf-8') as f:
      f.write(full_html)
    print(f"✓ Successfully converted example to HTML: '{html_output_path}'")
  except Exception as e:
    print(f"✗ Error writing HTML file: {e}")
    return

  # Save Confluence Storage Format file
  confluence_output_path = Path("converted_example_confluence.xml")
  try:
    with open(confluence_output_path, 'w', encoding='utf-8') as f:
      f.write(confluence_content)
    print(f"✓ Successfully converted example to Confluence format: '{confluence_output_path}'")
  except Exception as e:
    print(f"✗ Error writing Confluence file: {e}")
    return

  print(f"\nFiles created:")
  print(f"  • {html_output_path} - Open in browser to view HTML")
  print(f"  • {confluence_output_path} - Import into Confluence page")

  # Show preview of both formats
  print("\nHTML Content Preview:")
  print("-" * 30)
  print(html_content[:300] + "..." if len(html_content) > 300 else html_content)

  print("\nConfluence Storage Format Preview:")
  print("-" * 30)
  print(confluence_content[:400] + "..." if len(confluence_content) > 400 else confluence_content)


if __name__ == '__main__':
  main()
