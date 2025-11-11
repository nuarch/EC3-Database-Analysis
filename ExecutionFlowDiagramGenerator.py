import os
import re
import json
import openpyxl
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple

# Defaults
DEFAULT_INPUT_PATH = "export/stored_procedures_analysis_all_schemas.json"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_MAX_DEPTH = 100


def build_call_graph(recs: List[dict]) -> Tuple[Dict[str, Set[str]], Set[str]]:
  """
  Build a call graph from stored procedures where calls are exclusively to other known procedures.
  """
  edges = defaultdict(set)  # Adjacency list: source procedure -> called procedures
  known_procs = set()  # Set of all stored procedures

  # Extract information and build the list of known procedures
  for rec in recs:
    procedure_info = rec.get("procedure_info", {})
    schema = procedure_info.get("schema", "dbo").lower()
    name = procedure_info.get("name", "").lower()

    if schema and name:
      fqname = f"{schema}.{name}"  # Fully-qualified procedure name
      known_procs.add(fqname)

  # Build the call graph by parsing procedure definitions
  for rec in recs:
    procedure_info = rec.get("procedure_info", {})
    schema = procedure_info.get("schema", "dbo").lower()
    name = procedure_info.get("name", "").lower()
    definition = procedure_info.get("definition", "")

    if not schema or not name:
      continue

    fqname = f"{schema}.{name}"  # Fully-qualified name for the caller procedure

    # Extract calls using regex and only add known procedures to the graph
    for match in re.findall(r"\bEXEC(?:UTE)?\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?", definition, re.IGNORECASE):
      target_schema = match[0].lower() if match[0] else "dbo"
      target_name = match[1].lower()
      target_fqname = f"{target_schema}.{target_name}"

      if target_fqname in known_procs:  # Add only calls to known stored procedures
        edges[fqname].add(target_fqname)

  return edges, known_procs


def get_procedure_tree_depths(edges: Dict[str, Set[str]], max_depth: int) -> Dict[str, Dict[int, int]]:
  """
  For each stored procedure, calculate the count of calls at each depth.

  :param edges: Adjacency list representing the call graph.
  :param max_depth: The maximum depth to traverse the tree.
  :return: A dictionary with fully qualified procedure names as keys and the depth stats as values.
           Each depth stat is a dictionary where keys are depths and values are the number of calls at that depth.
  """
  def dfs(node: str, depth: int, depth_stats: Dict[int, int], visited: Set[str]):
    """
    Helper Depth-First Search to populate depth stats.

    :param node: Current stored procedure node.
    :param depth: Current depth in traversal.
    :param depth_stats: Dictionary tracking counts of calls at each depth.
    :param visited: Set of visited nodes to prevent cycles.
    """
    if depth > max_depth or node in visited:
      return
    visited.add(node)

    # Increment the count of calls at the current depth
    depth_stats[depth] += 1

    # Visit children
    for child in edges.get(node, []):
      dfs(child, depth + 1, depth_stats, visited)
    visited.remove(node)

  depths = {}  # Map: procedure -> depth stats (calls per depth)
  for procedure in edges.keys():
    depth_stats = defaultdict(int)  # Map: depth -> call count
    dfs(procedure, 1, depth_stats, set())  # Depth starts at 1 for children
    depths[procedure] = dict(depth_stats)  # Convert defaultdict to a standard dict

  return depths


def write_excel_summary(output_dir: str, procedure_depths: Dict[str, Dict[int, int]]):
  """
  Create an Excel sheet listing each stored procedure with its tree depth and call counts at each depth.

  :param output_dir: Directory to save the Excel file.
  :param procedure_depths: Dictionary of depth stats for each procedure.
  """
  # Create the Excel workbook and sheet
  workbook = openpyxl.Workbook()
  sheet = workbook.active
  sheet.title = "Procedure Depth Summary"

  # Write the header row
  header = ["Stored Procedure", "Max Depth"]
  max_depth = max(max(depths.keys()) for depths in procedure_depths.values() if depths)  # Determine max depth in graph
  for depth in range(1, max_depth + 1):
    header.append(f"Depth {depth} Calls")
  sheet.append(header)

  # Write data rows for each procedure
  for procedure, depth_stats in procedure_depths.items():
    # Get the max depth and calls at each depth
    max_depth = max(depth_stats.keys(), default=0)  # Default to 0 if the procedure has no calls
    row = [procedure, max_depth]
    for depth in range(1, max_depth + 1):
      row.append(depth_stats.get(depth, 0))  # Fill with 0 if no calls at that depth
    sheet.append(row)

  # Save the Excel file
  output_path = os.path.join(output_dir, "procedure_depth_summary.xlsx")
  os.makedirs(output_dir, exist_ok=True)
  workbook.save(output_path)
  print(f"Excel summary saved to: {output_path}")


def write_outputs(edges: Dict[str, Set[str]], output_dir: str, max_depth: int):
  """
  Write all outputs including individual files and depth stats in Excel.

  :param edges: Adjacency list representing the call graph.
  :param output_dir: The directory to save the outputs.
  :param max_depth: Maximum depth for depth calculation.
  """
  os.makedirs(output_dir, exist_ok=True)

  # Write full call graph to a Mermaid diagram
  mermaid_path = os.path.join(output_dir, "call_graph.mmd")
  with open(mermaid_path, "w", encoding="utf-8") as f:
    f.write("graph TD\n")
    for src, targets in edges.items():
      for tgt in targets:
        f.write(f"    {src} --> {tgt}\n")
  print(f"Full call graph written as Mermaid diagram: {mermaid_path}")

  # Write edge list as a CSV
  csv_path = os.path.join(output_dir, "call_graph.csv")
  with open(csv_path, "w", encoding="utf-8") as f:
    f.write("Source,Target\n")
    for src, targets in edges.items():
      for tgt in targets:
        f.write(f"{src},{tgt}\n")
  print(f"Call graph written as CSV: {csv_path}")

  # Calculate procedure depth stats and write Excel summary
  procedure_depths = get_procedure_tree_depths(edges, max_depth)
  write_excel_summary(output_dir, procedure_depths)


def main(input_path: str, output_dir: str, max_depth: int):
  """
  Main function to generate outputs.

  :param input_path: Input JSON file path.
  :param output_dir: Directory to output files.
  :param max_depth: Maximum depth for depth calculation.
  """
  # Load stored procedure data
  with open(input_path, "r", encoding="utf-8") as f:
    recs = json.load(f)

  # Build the call graph
  edges, _ = build_call_graph(recs)

  # Write outputs
  write_outputs(edges, output_dir, max_depth)


def run_with_interactive_menu():
  """Interactive menu to guide the user."""
  input_path = DEFAULT_INPUT_PATH
  output_dir = DEFAULT_OUTPUT_DIR

  while True:
    print("\n=== Interactive Menu ===")
    print("1. Set input JSON file path (current: {})".format(input_path))
    print("2. Set output directory path (current: {})".format(output_dir))
    print("3. Generate outputs (call graph, Excel)")
    print("0. Exit")
    print("========================")

    choice = input("Choose an option: ").strip()

    if choice == "1":
      new_input = input("Enter the input JSON file path: ").strip()
      if os.path.isfile(new_input):
        input_path = new_input
        print(f"Input file set to: {input_path}")
      else:
        print("Invalid file path. Try again.")
    elif choice == "2":
      new_output = input("Enter the output directory: ").strip()
      if new_output:
        output_dir = new_output
        print(f"Output directory set to: {output_dir}")
      else:
        print("Invalid directory. Try again.")
    elif choice == "3":
      print("\nGenerating outputs...")
      main(input_path, output_dir, DEFAULT_MAX_DEPTH)
    elif choice == "0":
      print("Exiting...")
      break
    else:
      print("Invalid option. Please try again.")


if __name__ == "__main__":
  run_with_interactive_menu()
