import os
import re
import json
import shutil
import openpyxl
import subprocess
import tempfile
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
        # Keep original casing from input; do not normalize to lower-case
        schema = procedure_info.get("schema", "dbo")
        name = procedure_info.get("name", "")
        definition = procedure_info.get("definition", "")

        if not schema or not name:
            continue

        # Fully qualified procedure name with original casing
        fqname = f"{schema}.{name}"
        known_procs.add(fqname)

        # Add procedure to the schema map (keyed by original schema casing)
        schema_map[schema].add(fqname)

        # Parse and map calls within the procedure definition
        for match in re.findall(r"\bEXEC(?:UTE)?\s+(?:\[?(\w+)\]?\.)?\[?(\w+)\]?", definition, re.IGNORECASE):
            # Keep captured schema and name casing as they appear in the definition
            target_schema = match[0] if match[0] else "dbo"
            target_name = match[1]
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


def write_schema_png_diagrams(edges: Dict[str, Set[str]], schema_map: Dict[str, Set[str]], output_dir: str):
    """
    Generate one transparent PNG call-graph diagram for each schema using Mermaid CLI (mmdc),
    only if the schema contains valid relationships.

    For each PNG image, also generate a Markdown file that explains the execution
    call paths in non-technical language, limited to the stored procedures that
    actually appear in the corresponding PNG.

    A temporary Mermaid (.mmd) file is created per schema and removed after PNG generation.

    :param edges: Adjacency list representing the call graph.
    :param schema_map: Map of schemas to the procedures they contain.
    :param output_dir: Directory to save the PNG diagrams and Markdown summaries.
    """
    schema_dir = os.path.join(output_dir, "schemas")
    os.makedirs(schema_dir, exist_ok=True)

    def friendly_proc_name(fqname: str) -> str:
        """
        Convert a fully-qualified procedure name (schema.name) into a friendlier label
        for non-technical readers.
        """
        parts = fqname.split(".", 1)
        if len(parts) == 2:
            schema, name = parts
            return f"procedure '{name}' in schema '{schema}'"
        return f"procedure '{fqname}'"

    def write_schema_markdown(
        schema: str,
        nodes_in_image: Set[str],
        local_edges: Dict[str, List[str]],
        local_incoming: Dict[str, List[str]],
        md_path: str,
    ):
        """
        Write a Markdown explanation of the execution paths for all procedures that
        appear in the PNG for this schema, using non-technical language.

        :param schema: Name of the schema whose PNG this description belongs to.
        :param nodes_in_image: Set of procedure names (fully qualified) that appear in the PNG.
        :param local_edges: Map of procedure -> list of procedures it calls (restricted to the PNG).
        :param local_incoming: Map of procedure -> list of procedures that call it (restricted to the PNG).
        :param md_path: File path where the Markdown should be written.
        """
        # Determine starting procedures: those that are not called by any other
        # procedure inside this PNG's subgraph.
        starting_procs = [p for p in nodes_in_image if not local_incoming.get(p)]
        starting_procs.sort()
        all_procs_sorted = sorted(nodes_in_image)

        lines: List[str] = []
        lines.append(f"# Execution flow for schema '{schema}'\n")
        lines.append(
            "This document explains, in everyday language, how the stored procedures that appear "
            "in the related diagram can trigger one another when they run.\n"
        )

        if not nodes_in_image:
            lines.append("There are no stored procedures shown in the diagram for this schema.\n")
        else:
            # Overview
            lines.append("## Overview\n")
            if starting_procs:
                lines.append(
                    "The following procedures act as **starting points** in this diagram. "
                    "They can be run directly and then may trigger other procedures shown:\n"
                )
                for proc in starting_procs:
                    lines.append(f"- {friendly_proc_name(proc)}")
                lines.append("")
            else:
                lines.append(
                    "All procedures in this diagram are part of one or more chains of calls. "
                    "There is no single obvious starting procedure in this picture.\n"
                )

            lines.append("## Detailed call paths\n")
            lines.append(
                "Below is a step-by-step description of how each procedure in the diagram "
                "can lead to others. Nested bullet points show what can be triggered next.\n"
            )

            described: Set[str] = set()

            def describe_proc(proc: str, indent: int, path: Set[str]):
                """
                Recursively describe how one procedure can trigger others in this PNG,
                using nested bullet points and avoiding infinite loops.
                """
                indent_spaces = "  " * indent
                display_name = friendly_proc_name(proc)

                if proc in path:
                    # Cycle detected within the subgraph
                    lines.append(
                        f"{indent_spaces}- {display_name} (this procedure can eventually call itself again "
                        f"through a loop of calls shown in the diagram)"
                    )
                    return

                if proc not in described:
                    described.add(proc)

                children = sorted(local_edges.get(proc, []))
                if not children:
                    lines.append(
                        f"{indent_spaces}- {display_name} "
                        f"(in this diagram, this procedure does not trigger any other recorded procedures)"
                    )
                    return

                lines.append(f"{indent_spaces}- {display_name} can trigger:")
                new_path = set(path)
                new_path.add(proc)
                for child in children:
                    describe_proc(child, indent + 1, new_path)

            # First describe starting procedures
            if starting_procs:
                for proc in starting_procs:
                    describe_proc(proc, 0, set())

            # Then describe any remaining procedures that appear in the image but
            # were not already fully described from a starting point.
            remaining = [p for p in all_procs_sorted if p not in described]
            if remaining:
                lines.append("")
                lines.append(
                    "## Additional procedures\n"
                    "The following procedures are also shown in the diagram but mainly appear "
                    "in the middle of call chains:"
                )
                for proc in remaining:
                    describe_proc(proc, 0, set())

        with open(md_path, "w", encoding="utf-8") as md_file:
            md_file.write("\n".join(lines))

    for schema, procedures in schema_map.items():
        mermaid_content = "graph TD\n"
        graph_has_content = False

        # These structures are restricted to what appears in THIS schema's PNG
        nodes_in_image: Set[str] = set()
        local_edges: Dict[str, List[str]] = defaultdict(list)
        local_incoming: Dict[str, List[str]] = defaultdict(list)

        for procedure in procedures:
            for target_proc in edges.get(procedure, []):
                # Every edge we draw in the diagram is also reflected in the local structures
                mermaid_content += f"    {procedure} --> {target_proc}\n"
                graph_has_content = True

                # Track nodes and local relationships ONLY for procedures that appear in this image
                nodes_in_image.add(procedure)
                nodes_in_image.add(target_proc)
                local_edges[procedure].append(target_proc)
                local_incoming[target_proc].append(procedure)

        if not graph_has_content:
            # No relationships to draw or explain for this schema
            continue

        safe_schema_name = re.sub(r"[^\w]", "_", schema)
        png_path = os.path.join(schema_dir, f"{safe_schema_name}.png")
        md_path = os.path.join(schema_dir, f"{safe_schema_name}.md")

        # Create a temporary Mermaid file for this schema
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mmd", mode="w", encoding="utf-8") as tmp_mmd:
            tmp_mmd_path = tmp_mmd.name
            tmp_mmd.write(mermaid_content)

        try:
            # Generate the PNG diagram
            subprocess.run(
                ["mmdc", "-i", tmp_mmd_path, "-o", png_path, "-b", "transparent", "-w", "8000", "-H", "4800"],
                check=True,
            )
            print(f"Transparent PNG diagram for schema '{schema}' saved to: {png_path}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to generate PNG for schema '{schema}': {e}")
        finally:
            # Clean up the temporary .mmd file
            try:
                os.remove(tmp_mmd_path)
            except OSError:
                pass

        # Generate the Markdown explanation limited to what appears in this PNG
        write_schema_markdown(schema, nodes_in_image, local_edges, local_incoming, md_path)
        print(f"Markdown summary for schema '{schema}' saved to: {md_path}")

    print(f"Transparent PNG diagrams and Markdown summaries for schemas saved to: {schema_dir}")


def write_outputs(edges: Dict[str, Set[str]], schema_map: Dict[str, Set[str]], output_dir: str, max_depth: int):
    """
    Write all outputs including schema-based PNG diagrams, Excel summary, and CSV.

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

    # Write individual schema transparent PNG diagrams (no .mmd files)
    write_schema_png_diagrams(edges, schema_map, output_dir)

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
