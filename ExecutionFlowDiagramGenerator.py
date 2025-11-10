import json
import os
import re
import argparse
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple, Any

DEFAULT_MAX_DEPTH = 50

EXEC_PATTERN = re.compile(
    r"\bEXEC(?:UTE)?\s+(?P<target>(?:\[?\w+\]?\.){0,2}\[?\w+\]?)(?!\s*=)",
    re.IGNORECASE
)

BRACKET_STRIP = re.compile(r"[\[\]]")

SYSTEM_PROC_PREFIXES = (
    'sp_', 'xp_', 'msdb..', 'sys.', 'dbo.sp_', 'sys.sp_', 'sys.xp_'
)


def normalize_name(name: str) -> str:
    # Remove brackets and compress multiple dots
    name = BRACKET_STRIP.sub('', name.strip())
    parts = [p for p in name.split('.') if p]
    # Accept last two parts as schema.proc; if only one, assume dbo
    if len(parts) >= 2:
        schema = parts[-2]
        proc = parts[-1]
    else:
        schema = 'dbo'
        proc = parts[-1] if parts else ''
    return f"{schema}.{proc}".lower()


def is_system_proc(fqname: str) -> bool:
    lname = fqname.lower()
    return lname.startswith(('sys.', 'msdb.', 'master.', 'tempdb.')) or any(lname.startswith(p) for p in SYSTEM_PROC_PREFIXES)


def extract_exec_calls(definition: str) -> List[str]:
    calls = []
    if not definition:
        return calls
    for m in EXEC_PATTERN.finditer(definition):
        target = m.group('target')
        # Skip EXEC (@sql) and similar dynamic
        if target.lstrip().startswith('@'):
            continue
        calls.append(target)
    return calls


def load_sp_json(path: str) -> List[dict]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def build_call_graph(recs: List[dict]) -> Tuple[Dict[str, Set[str]], Set[str], Dict[str, str], Set[str], Dict[str, Set[str]]]:
    name_to_def: Dict[str, str] = {}
    edges: Dict[str, Set[str]] = defaultdict(set)
    dynamic_calls: Set[str] = set()
    unresolved_by_source: Dict[str, Set[str]] = defaultdict(set)

    # Collect all procedure names present so we can filter internal calls
    known: Set[str] = set()
    for rec in recs:
        info = rec.get('procedure_info') or {}
        schema = info.get('schema') or info.get('ROUTINE_SCHEMA')
        name = info.get('name') or info.get('ROUTINE_NAME')
        if not schema or not name:
            continue
        fqname = f"{schema}.{name}".lower()
        known.add(fqname)

    for rec in recs:
        info = rec.get('procedure_info') or {}
        schema = info.get('schema') or info.get('ROUTINE_SCHEMA')
        name = info.get('name') or info.get('ROUTINE_NAME')
        definition = info.get('definition') or rec.get('definition') or ''
        if not schema or not name:
            continue
        source = f"{schema}.{name}".lower()
        name_to_def[source] = definition
        raw_calls = extract_exec_calls(definition)
        if not raw_calls:
            continue
        for raw in raw_calls:
            fq = normalize_name(raw)
            if not fq or is_system_proc(fq):
                continue
            # Track dynamic possibilities if exec uses concatenation etc. (rough heuristic)
            if any(tok in raw for tok in ['+', '@', 'CONCAT', 'QUOTENAME']):
                dynamic_calls.add(source)
            edges[source].add(fq)
            if fq not in known:
                unresolved_by_source[source].add(fq)
    return edges, dynamic_calls, name_to_def, known, unresolved_by_source


def to_mermaid(edges: Dict[str, Set[str]]) -> str:
    lines = ["graph TD"]
    # Create deterministic order
    all_nodes: Set[str] = set(edges.keys()) | {t for ts in edges.values() for t in ts}
    # ensure standalone nodes appear
    for n in sorted(all_nodes):
        if n not in edges or not edges[n]:
            lines.append(f"    {node_id(n)}[{label(n)}]")
    for src in sorted(edges.keys()):
        targets = sorted(edges[src])
        for tgt in targets:
            lines.append(f"    {node_id(src)}[{label(src)}] --> {node_id(tgt)}[{label(tgt)}]")
    return "\n".join(lines)


def label(fqname: str) -> str:
    return fqname


def node_id(fqname: str) -> str:
    # Mermaid node ids must be simple; replace non-alnum with _
    return re.sub(r'[^A-Za-z0-9_]', '_', fqname)


def build_reachable_subgraph(root: str, edges: Dict[str, Set[str]], max_depth: int, known: Set[str], unresolved_by_source: Dict[str, Set[str]]):
    visited: Set[str] = set()
    sub_edges: Dict[str, Set[str]] = defaultdict(set)
    stack: List[Tuple[str, int]] = [(root, 0)]
    while stack:
        node, depth = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        if depth >= max_depth:
            continue
        for child in edges.get(node, set()):
            sub_edges[node].add(child)
            if child not in visited:
                stack.append((child, depth + 1))
    # Ensure unresolved targets appear as nodes too
    for src, tgts in unresolved_by_source.items():
        if src in visited:
            for tgt in tgts:
                sub_edges[src].add(tgt)
    return sub_edges


def enumerate_paths(root: str, edges: Dict[str, Set[str]], max_depth: int, known: Set[str], unresolved_by_source: Dict[str, Set[str]]):
    paths: List[Dict[str, Any]] = []

    def dfs(node: str, path: List[str], onstack: Set[str], depth: int):
        if depth > max_depth:
            paths.append({
                'root': root,
                'path': path.copy(),
                'depth': len(path) - 1,
                'terminal': 'max_depth',
                'cycle': False,
                'unresolved': False
            })
            return
        # Explore children: include both known edges and unresolved-only if any
        children = set(edges.get(node, set()))
        if node in unresolved_by_source:
            children = children | set(unresolved_by_source[node])
        if not children:
            paths.append({
                'root': root,
                'path': path.copy(),
                'depth': len(path) - 1,
                'terminal': 'leaf',
                'cycle': False,
                'unresolved': False
            })
            return
        for child in sorted(children):
            if child in onstack:
                # report cycle
                paths.append({
                    'root': root,
                    'path': path + [child],
                    'depth': len(path),
                    'terminal': 'cycle',
                    'cycle': True,
                    'unresolved': False
                })
                continue
            # If child is unresolved (not in known), end path there
            if child not in known:
                paths.append({
                    'root': root,
                    'path': path + [child],
                    'depth': len(path),
                    'terminal': 'unresolved',
                    'cycle': False,
                    'unresolved': True
                })
                continue
            onstack.add(child)
            dfs(child, path + [child], onstack, depth + 1)
            onstack.remove(child)

    dfs(root, [root], {root}, 0)
    return paths


def trees_to_json(trees: Dict[str, Dict[str, Any]]) -> str:
    return json.dumps(trees, indent=2)


def write_outputs(edges: Dict[str, Set[str]], dynamic_sources: Set[str], out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    mermaid = to_mermaid(edges)
    mmd_path = os.path.join(out_dir, 'execution_flow_mermaid.mmd')
    with open(mmd_path, 'w', encoding='utf-8') as f:
        f.write(mermaid)

    md_path = os.path.join(out_dir, 'execution_flow_diagram.md')
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write("## Stored Procedure Execution Flow (EC3Database_Analysis)\n\n")
        f.write("```mermaid\n")
        f.write(mermaid)
        f.write("\n```\n\n")
        if dynamic_sources:
            f.write("Notes:\n\n")
            f.write("- Some procedures may invoke others via dynamic SQL; these sources were detected heuristically and may be incomplete: \n")
            for s in sorted(dynamic_sources):
                f.write(f"  - {s}\n")

    # Also emit CSV of edges
    csv_path = os.path.join(out_dir, 'execution_flow_edges.csv')
    with open(csv_path, 'w', encoding='utf-8') as f:
        f.write('source,target\n')
        for src in sorted(edges.keys()):
            for tgt in sorted(edges[src]):
                f.write(f'{src},{tgt}\n')


def safe_filename(name: str) -> str:
    return re.sub(r'[^A-Za-z0-9_.-]', '_', name)


def write_full_tree_outputs(
    out_dir: str,
    edges: Dict[str, Set[str]],
    known: Set[str],
    unresolved_by_source: Dict[str, Set[str]],
    roots: List[str],
    max_depth: int
):
    trees_dir = os.path.join(out_dir, 'trees')
    os.makedirs(trees_dir, exist_ok=True)

    all_paths: List[Dict[str, Any]] = []
    trees_json: Dict[str, Dict[str, List[str]]] = {}

    # For cycle reporting
    cycles: List[Dict[str, Any]] = []

    # Track empty tree files we delete to keep the directory clean
    deleted_empty_files: List[str] = []
    skipped_empty_roots: List[str] = []

    for root in sorted(roots):
        sub_edges = build_reachable_subgraph(root, edges, max_depth, known, unresolved_by_source)
        # JSON tree (adjacency list for this root)
        trees_json[root] = {k: sorted(list(v)) for k, v in sorted(sub_edges.items())}

        # Determine if this tree is empty (no nodes/edges reachable)
        is_empty_tree = len(sub_edges) == 0

        root_file = os.path.join(trees_dir, f"{safe_filename(root)}.mmd")
        root_md_file = os.path.join(trees_dir, f"{safe_filename(root)}.md")

        if is_empty_tree:
            # If previously generated files exist for this empty tree, delete them
            for p in (root_file, root_md_file):
                if os.path.exists(p):
                    try:
                        os.remove(p)
                        deleted_empty_files.append(p)
                    except Exception:
                        # ignore deletion errors silently to avoid failing the whole run
                        pass
            skipped_empty_roots.append(root)
        else:
            # Mermaid per root
            mermaid = to_mermaid(sub_edges)
            # Write raw Mermaid for diagram tools
            with open(root_file, 'w', encoding='utf-8') as f:
                f.write(mermaid)
            # Write Markdown with embedded Mermaid block for easy viewing
            with open(root_md_file, 'w', encoding='utf-8') as f:
                f.write(f"## Execution Tree — {root}\n\n")
                f.write("```mermaid\n")
                f.write(mermaid)
                f.write("\n```\n")

        # Paths enumeration
        paths = enumerate_paths(root, edges, max_depth, known, unresolved_by_source)
        all_paths.extend(paths)
        # Capture cycles for this root
        for p in paths:
            if p.get('cycle'):
                cycles.append(p)

    # Write trees.json
    with open(os.path.join(out_dir, 'trees.json'), 'w', encoding='utf-8') as f:
        f.write(trees_to_json(trees_json))

    # Write an optional cleanup report if we skipped/deleted empties
    if deleted_empty_files or skipped_empty_roots:
        report_path = os.path.join(out_dir, 'empty_trees_cleanup_report.md')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('# Empty Trees Cleanup Report\n\n')
            if skipped_empty_roots:
                f.write('## Skipped roots (no tree to emit)\n')
                for r in skipped_empty_roots:
                    f.write(f'- {r}\n')
                f.write('\n')
            if deleted_empty_files:
                f.write('## Deleted previously generated empty files\n')
                for p in deleted_empty_files:
                    f.write(f'- {p}\n')
                f.write('\n')

    # Write all_paths.csv
    paths_csv = os.path.join(out_dir, 'all_paths.csv')
    with open(paths_csv, 'w', encoding='utf-8') as f:
        f.write('root,depth,terminal,cycle,unresolved,path\n')
        for p in all_paths:
            path_str = ' > '.join(p['path'])
            f.write(f"{p['root']},{p['depth']},{p['terminal']},{int(p['cycle'])},{int(p['unresolved'])},\"{path_str}\"\n")

    # cycles_report.md
    cycles_md = os.path.join(out_dir, 'cycles_report.md')
    with open(cycles_md, 'w', encoding='utf-8') as f:
        f.write('# Cycles Detected in Stored Procedure Calls\n\n')
        if not cycles:
            f.write('No cycles detected.\n')
        else:
            # de-duplicate cycle signatures by the last edge forming the cycle
            seen = set()
            for c in cycles:
                path_tuple = tuple(c['path'])
                sig = (c['root'], path_tuple[-2] if len(path_tuple) >= 2 else path_tuple[-1], path_tuple[-1])
                if sig in seen:
                    continue
                seen.add(sig)
                f.write(f"- Root: {c['root']} | Cycle path: {' > '.join(c['path'])}\n")

    # unresolved_calls.md
    unresolved_md = os.path.join(out_dir, 'unresolved_calls.md')
    with open(unresolved_md, 'w', encoding='utf-8') as f:
        f.write('# Unresolved Procedure Calls (likely cross-DB or missing)\n\n')
        if not unresolved_by_source:
            f.write('None.\n')
        else:
            for src in sorted(unresolved_by_source.keys()):
                f.write(f"- {src}:\n")
                for tgt in sorted(unresolved_by_source[src]):
                    f.write(f"  - {tgt}\n")



def parse_roots_arg(roots_arg: str, known: Set[str]) -> List[str]:
    if not roots_arg or roots_arg.lower() == 'all':
        return sorted(list(known))
    if roots_arg.startswith('@'):
        path = roots_arg[1:]
        if os.path.isfile(path):
            with open(path, 'r', encoding='utf-8') as f:
                roots = [line.strip().lower() for line in f if line.strip()]
                return [r for r in roots if r in known]
    # comma-separated list
    roots = [r.strip().lower() for r in roots_arg.split(',') if r.strip()]
    return [r for r in roots if r in known]


def main():
    parser = argparse.ArgumentParser(description='Generate execution flow diagram from stored procedures JSON export')
    parser.add_argument('--input', required=True, help='Path to stored_procedures_analysis_all_schemas.json')
    parser.add_argument('--output-dir', required=True, help='Directory to write outputs')
    parser.add_argument('--max-depth', type=int, default=DEFAULT_MAX_DEPTH, help='Maximum traversal depth (default 50)')
    parser.add_argument('--roots', type=str, default='all', help="Comma-separated list of root procedures (schema.proc), '@file' to read list, or 'all'")
    args = parser.parse_args()

    recs = load_sp_json(args.input)
    edges, dynamic_sources, _name_to_def, known, unresolved_by_source = build_call_graph(recs)

    # Write overall (direct) diagram and edges CSV
    write_outputs(edges, dynamic_sources, args.output_dir)

    # Determine roots for full tree expansion
    roots = parse_roots_arg(args.roots, known)
    if not roots:
        roots = sorted(list(known))

    # Write full tree outputs per requirements
    write_full_tree_outputs(
        out_dir=args.output_dir,
        edges=edges,
        known=known,
        unresolved_by_source=unresolved_by_source,
        roots=roots,
        max_depth=args.max_depth
    )

    print(f"Wrote direct graph and full trees to {args.output_dir}")


if __name__ == '__main__':
    main()
