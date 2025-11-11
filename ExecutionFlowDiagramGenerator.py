import os
import re
import json
import shutil
import openpyxl
from collections import defaultdict
from typing import Dict, List, Set, Tuple

# Defaults
DEFAULT_INPUT_PATH = "export/stored_procedures_analysis_all_schemas.json"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_MAX_DEPTH = 100


def wipe_output_directory(output_dir: str):
    """
    Wipe the existing output directory by deleting its contents, and recreate it.

    :param output_dir: The directory to wipe and recreate.
    """
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)  # Delete the existing directory and all its contents
    os.makedirs(output_dir, exist_ok=True)  # Recreate the directory
    print(f"Output directory wiped and recreated: {output_dir}")


def build_call_graph(recs: List[dict]) -> Tuple[Dict[str, Set[str]], Set[str], Dict[str, Set[str]]]:
    """
    Build a call graph from stored procedures, ensuring calls are exclusively to other known procedures,
    while grouping stored procedures by their schema.

    :param recs: List of stored procedure records from JSON.
    :return: A tuple containing:
        - edges: Adjacency list representing the graph (e.g., {procA -> {procB, procC}})
        - known_procs: Set of all known stored procedures.
        - schema_map: Map of schemas to the procedures they contain.
    """
    edges = defaultdict(set)  # Adjacency list: source procedure -> called procedures
    schema_map = defaultdict(set)  # Schema map: schema -> set of procedures
    known_procs = set()  # Set of all stored procedures

    for rec in recs:
        procedure_info = rec.get("procedure_info", {})
        schema = procedure_info.get("schema", "dbo").lower()
        name = procedure_info.get("name", "").lower()
        definition = procedure_info.get("definition", "")

        if not schema or not name:
            continue

        # Fully qualified procedure name
        fqname = f"{schema}.{name}"
        known_procs.add(fqname)

        # Add procedure to the schema map
        schema_map[schema].add(fqname)

        # Parse and map calls within the procedure definition
        for match in re.findall(r"\bEXEC(?:UTE)?\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?", definition, re.IGNORECASE):
            target_schema = match[0].lower() if match[0] else "dbo"
            target_name = match[1].lower()
            target_fqname = f"{target_schema}.{target_name}"
            if target_fqname in known_procs:  # Only link to known stored procedures
                edges[fqname].add(target_fqname)

    return edges, known_procs, schema_map


def get_procedure_tree_depths(edges: Dict[str, Set[str]], max_depth: int) -> Dict[str, Dict[int, int]]:
    """
    Calculate the tree depth for each procedure and the number of calls made at each depth.

    :param edges: Adjacency list representing the call graph.
    :param max_depth: The maximum depth to traverse.
    :return: A dictionary with stored procedures as keys and their depth stats as values.
             Each depth stat is a dictionary where keys are depths and values are the number of calls at that depth.
    """
    def dfs(node: str, depth: int, depth_stats: Dict[int, int], visited: Set[str]):
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
        dfs(procedure, 1, depth_stats, set())  # Start depth at 1
        depths[procedure] = dict(depth_stats)  # Convert defaultdict to standard dict

    return depths


def write_excel_summary(output_dir: str, procedure_depths: Dict[str, Dict[int, int]]):
    """
    Create an Excel sheet listing each stored procedure with its tree depth and call counts at each depth.

    :param output_dir: Directory to save the Excel file.
    :param procedure_depths: Dictionary of depth stats for each procedure.
    """
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Procedure Depth Summary"

    # Write the header row
    header = ["Stored Procedure", "Max Depth"]
    max_global_depth = max((max(depths.keys()) for depths in procedure_depths.values() if depths), default=0)
    for depth in range(1, max_global_depth + 1):
        header.append(f"Depth {depth} Calls")
    sheet.append(header)

    # Write data rows for each procedure
    for procedure, depth_stats in procedure_depths.items():
        max_depth = max(depth_stats.keys(), default=0)
        row = [procedure, max_depth]
        for depth in range(1, max_global_depth + 1):
            row.append(depth_stats.get(depth, 0))  # Fill with 0 if no calls at this depth
        sheet.append(row)

    # Save the Excel file
    output_path = os.path.join(output_dir, "procedure_depth_summary.xlsx")
    os.makedirs(output_dir, exist_ok=True)
    workbook.save(output_path)
    print(f"Excel summary saved to: {output_path}")


def write_schema_mermaid_diagrams(edges: Dict[str, Set[str]], schema_map: Dict[str, Set[str]], output_dir: str):
    """
    Generate one Mermaid diagram for each schema and save it to a separate file,
    only if the schema contains valid relationships.

    :param edges: Adjacency list representing the call graph.
    :param schema_map: Map of schemas to the procedures they contain.
    :param output_dir: Directory to save the Mermaid diagrams.
    """
    schema_dir = os.path.join(output_dir, "schemas")
    os.makedirs(schema_dir, exist_ok=True)

    for schema, procedures in schema_map.items():
        # Create a Mermaid graph for the schema
        mermaid_content = "graph TD\n"
        graph_has_content = False  # Flag to check if the schema has any valid relationships

        for procedure in procedures:
            for target_proc in edges.get(procedure, []):
                if target_proc in procedures:  # Only include procedures within the same schema
                    mermaid_content += f"    {procedure} --> {target_proc}\n"
                    graph_has_content = True  # Mark that the graph will have content

        if graph_has_content:  # Only write the file if the graph has content
            safe_schema_name = re.sub(r"[^\w]", "_", schema)  # File-safe schema name
            file_path = os.path.join(schema_dir, f"{safe_schema_name}.mmd")

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(mermaid_content)

    print(f"Mermaid diagrams for schemas saved to: {schema_dir}")


def write_outputs(edges: Dict[str, Set[str]], schema_map: Dict[str, Set[str]], output_dir: str, max_depth: int):
    """
    Write all outputs including schema-based Mermaid diagrams, Excel summary, and CSV.

    :param edges: Adjacency list representing the call graph.
    :param schema_map: Map of schemas to the procedures they contain.
    :param output_dir: Directory to save outputs.
    :param max_depth: Maximum depth for depth calculation.
    """
    # Write CSV file of the call graph if it has content
    if edges:
        csv_path = os.path.join(output_dir, "call_graph.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            f.write("Source,Target\n")
            for src, targets in edges.items():
                for tgt in targets:
                    f.write(f"{src},{tgt}\n")
        print(f"Call graph written as CSV: {csv_path}")

    # Write individual schema Mermaid diagrams
    write_schema_mermaid_diagrams(edges, schema_map, output_dir)

    # Write procedure depth statistics to an Excel file
    procedure_depths = get_procedure_tree_depths(edges, max_depth)
    write_excel_summary(output_dir, procedure_depths)


def main(input_path: str, output_dir: str, max_depth: int):
    """
    Main function to generate outputs (excluding full call graph Mermaid file).

    :param input_path: Input JSON file path.
    :param output_dir: Directory to output files.
    :param max_depth: Maximum depth for depth calculation.
    """
    # Wipe the output directory
    wipe_output_directory(output_dir)

    # Load stored procedure data
    with open(input_path, "r", encoding="utf-8") as f:
        recs = json.load(f)

    # Build the call graph and schema map
    edges, _, schema_map = build_call_graph(recs)

    # Write the outputs
    write_outputs(edges, schema_map, output_dir, max_depth)


def run_with_interactive_menu():
    """Interactive menu to guide the user."""
    input_path = DEFAULT_INPUT_PATH
    output_dir = DEFAULT_OUTPUT_DIR

    while True:
        print("\n=== Interactive Menu ===")
        print("1. Set input JSON file path (current: {})".format(input_path))
        print("2. Set output directory path (current: {})".format(output_dir))
        print("3. Generate outputs (schema diagrams, CSV, Excel file)")
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
