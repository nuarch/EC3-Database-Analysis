import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# -----------------------------
# Data Models
# -----------------------------

@dataclass
class ProcedureInfo:
  schema: str
  name: str
  definition: str


@dataclass
class ProcedureAnalysisEntry:
  """Structured representation of one entry in stored_procedures_analysis_all_schemas.json."""
  schema: str
  name: str
  definition: str
  raw_entry: Dict[str, Any]


@dataclass
class ProcedureFlowDocumentation:
  schema: str
  name: str
  title: str
  mermaid_flowchart: str
  high_level_summary: str
  step_by_step_flow: List[str]
  # Direct children are still useful metadata, but all their logic is embedded
  # into the diagram and narrative; there is no separate section any more.
  called_procedures: List[str]


# -----------------------------
# Main Documenter Class
# -----------------------------

class StoredProcedureFlowDocumenter:
  """
  Reads:
    1) /export/stored_procedures_analysis_all_schemas.json
    2) /export/stored_procedures_to_process_logic.json

  And produces:
    - A Markdown document that describes each selected stored procedure
      and the complete call tree (including other procedures it calls)
      as a flow diagram and a step‑by‑step narrative.
  """

  def __init__(
      self,
      all_procedures_path: Optional[str] = None,
      to_process_path: Optional[str] = None,
      output_markdown_path: Optional[str] = None,
  ):
    # Base /export folder (sibling to this script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    export_dir = os.path.join(script_dir, "export")

    # Default to files inside the /export folder next to this script
    self.all_procedures_path = all_procedures_path or os.path.join(
      export_dir, "stored_procedures_analysis_all_schemas.json"
    )
    self.to_process_path = to_process_path or os.path.join(
      export_dir, "stored_procedures_to_process_logic.json"
    )
    self.output_markdown_path = output_markdown_path or os.path.join(
      export_dir, "stored_procedures_logic_flows.md"
    )

    # Index: (schema.lower(), name.lower()) -> ProcedureAnalysisEntry
    self.procedure_index: Dict[Tuple[str, str], ProcedureAnalysisEntry] = {}

  # -----------------------------
  # Loading JSON
  # -----------------------------

  def load_all_procedures(self) -> None:
    """Load all procedures from stored_procedures_analysis_all_schemas.json."""
    if not os.path.exists(self.all_procedures_path):
      raise FileNotFoundError(f"All-procedures file not found: {self.all_procedures_path}")

    with open(self.all_procedures_path, "r", encoding="utf-8") as f:
      data = json.load(f)

    logger.info("Loaded %d procedure analysis entries", len(data))

    for entry in data:
      proc_info = entry.get("procedure_info") or {}
      schema = proc_info.get("schema")
      name = proc_info.get("name")
      definition = proc_info.get("definition")

      if not schema or not name or not definition:
        continue

      key = (schema.lower(), name.lower())
      self.procedure_index[key] = ProcedureAnalysisEntry(
        schema=schema,
        name=name,
        definition=definition,
        raw_entry=entry,
      )

    logger.info("Indexed %d procedures from all-schemas file", len(self.procedure_index))

  def load_procedures_to_process(self) -> List[ProcedureInfo]:
    """
    Load the list of procedures whose logic/flow we should document.

    Expected JSON format examples (flexible):

    1) Simple list of objects:
       [
         { "schema": "Batch", "procedure-name": "usp_StarProcess" },
         { "schema": "Billing", "procedure-name": "usp_AddNewAgency" }
       ]

    2) Or with keys like 'schema' and 'name':
       [
         { "schema": "Batch", "name": "usp_StarProcess" }
       ]
    """
    if not os.path.exists(self.to_process_path):
      raise FileNotFoundError(f"Procedures-to-process file not found: {self.to_process_path}")

    with open(self.to_process_path, "r", encoding="utf-8") as f:
      data = json.load(f)

    procedures: List[ProcedureInfo] = []

    for item in data:
      schema = item.get("schema") or item.get("Schema")
      name = item.get("procedure-name") or item.get("procedure_name") or item.get("name")
      if not schema or not name:
        continue

      key = (schema.lower(), name.lower())
      analysis_entry = self.procedure_index.get(key)
      if not analysis_entry:
        logger.warning("Procedure from to-process list not found in index: %s.%s", schema, name)
        continue

      procedures.append(
        ProcedureInfo(
          schema=analysis_entry.schema,
          name=analysis_entry.name,
          definition=analysis_entry.definition,
        )
      )

    logger.info("Resolved %d procedures to document (out of %d requested)", len(procedures), len(data))
    return procedures

  # -----------------------------
  # Flow & Logic Extraction
  # -----------------------------

  EXEC_PATTERN = re.compile(
    r"""
        \bEXEC(?:UTE)?        # EXEC or EXECUTE
        \s+
        (?:
            \[?(?P<schema>\w+)\]?\.\[?(?P<name>\w+)\]?  # Optional [schema].[name]
            |
            \[?(?P<name_only>\w+)\]?                   # Or just [name]
        )
        """,
    re.IGNORECASE | re.VERBOSE,
    )

  BEGIN_PATTERN = re.compile(r"\bBEGIN\b", re.IGNORECASE)
  END_PATTERN = re.compile(r"\bEND\b", re.IGNORECASE)
  IF_PATTERN = re.compile(r"\bIF\b", re.IGNORECASE)
  TRY_PATTERN = re.compile(r"\bBEGIN\s+TRY\b", re.IGNORECASE)
  CATCH_PATTERN = re.compile(r"\bBEGIN\s+CATCH\b", re.IGNORECASE)
  INSERT_PATTERN = re.compile(r"\bINSERT\s+INTO\b", re.IGNORECASE)
  UPDATE_PATTERN = re.compile(r"\bUPDATE\b", re.IGNORECASE)
  DELETE_PATTERN = re.compile(r"\bDELETE\b", re.IGNORECASE)
  SELECT_PATTERN = re.compile(r"\bSELECT\b", re.IGNORECASE)
  TRUNCATE_PATTERN = re.compile(r"\bTRUNCATE\s+TABLE\b", re.IGNORECASE)

  def find_called_procedures(self, proc: ProcedureInfo) -> List[str]:
    """
    Very simple scan for EXEC/EXECUTE to identify called procedures.
    Returns list like ["SchemaName.ProcName", "OtherProc"].
    """
    text = proc.definition
    calls: List[str] = []

    for match in self.EXEC_PATTERN.finditer(text):
      schema = match.group("schema")
      name = match.group("name") or match.group("name_only")
      if not name:
        continue

      if schema:
        full = f"{schema}.{name}"
      else:
        full = name

      # Avoid listing the procedure calling itself as a separate “call”
      if full.lower() == f"{proc.schema}.{proc.name}".lower() or full.lower() == proc.name.lower():
        continue

      if full not in calls:
        calls.append(full)

    return calls

  def classify_main_actions(self, proc: ProcedureInfo) -> List[str]:
    """
    Heuristic-based classification of the main types of actions in the procedure,
    used to build a human-friendly description.
    """
    code = proc.definition

    actions: List[str] = []

    if self.TRUNCATE_PATTERN.search(code):
      actions.append("clears one or more tables before new data is loaded")

    if self.INSERT_PATTERN.search(code) and "archive" in code.lower():
      actions.append("moves or archives data from one place to another")

    if self.INSERT_PATTERN.search(code) and "log" in code.lower():
      actions.append("records information into a log table")

    if self.UPDATE_PATTERN.search(code):
      actions.append("updates existing records to reflect new status or results")

    if self.SELECT_PATTERN.search(code):
      actions.append("retrieves data needed to decide what to do next")

    if self.IF_PATTERN.search(code):
      actions.append("makes decisions based on conditions")

    if self.TRY_PATTERN.search(code) or self.CATCH_PATTERN.search(code):
      actions.append("handles errors and records when something goes wrong")

    # Remove duplicates while preserving order
    seen = set()
    unique_actions: List[str] = []
    for act in actions:
      if act not in seen:
        seen.add(act)
        unique_actions.append(act)

    return unique_actions

  def _lookup_procedure_by_full_name(self, full_name: str) -> Optional[ProcedureInfo]:
    """
    Given a string like 'Schema.ProcName' or just 'ProcName', try to
    find the corresponding ProcedureInfo from the index.
    """
    schema = None
    name = None

    if "." in full_name:
      parts = full_name.split(".", 1)
      schema = parts[0].strip("[]")
      name = parts[1].strip("[]")
    else:
      # If schema is not given, we can't reliably guess it
      return None

    key = (schema.lower(), name.lower())
    entry = self.procedure_index.get(key)
    if not entry:
      return None

    return ProcedureInfo(schema=entry.schema, name=entry.name, definition=entry.definition)

  def _summarize_actions_sentence(self, proc: ProcedureInfo) -> Optional[str]:
    """
    Short non‑technical sentence summarizing what a given procedure does,
    based on its main actions.
    """
    actions = self.classify_main_actions(proc)
    if not actions:
      return None

    if len(actions) == 1:
      actions_text = actions[0]
    else:
      actions_text = ", ".join(actions[:-1]) + ", and " + actions[-1]

    return f"Inside **{proc.schema}.{proc.name}**, it {actions_text}."

  def generate_high_level_summary(
      self,
      proc: ProcedureInfo,
      called_procs: List[str],
  ) -> str:
    """Create a non-technical, high-level summary paragraph for the whole call tree."""
    pieces: List[str] = []

    pieces.append(
      f"This stored procedure **{proc.schema}.{proc.name}** acts as a workflow step inside the billing system. "
      f"It may call other stored procedures, so together they form a complete chain of actions."
    )

    root_actions = self.classify_main_actions(proc)
    if root_actions:
      if len(root_actions) == 1:
        actions_text = root_actions[0]
      else:
        actions_text = ", ".join(root_actions[:-1]) + ", and " + root_actions[-1]
      pieces.append(f"On its own, **{proc.schema}.{proc.name}** {actions_text}.")

    if called_procs:
      pieces.append(
        "In addition, it calls other stored procedures. Each of these performs its own part of the job, "
        "so together they complete the end‑to‑end flow."
      )
    else:
      pieces.append(
        "This particular procedure does not call any others; all of its work happens inside this one routine."
      )

    pieces.append(
      "Overall, you can think of the full call tree as a checklist of smaller tasks that are carried out in order, "
      "so that the data ends up in the correct state for later steps and reports."
    )

    return " ".join(pieces)

  # -----------------------------
  # Recursive narrative builder
  # -----------------------------

  def _append_steps_for_proc(
      self,
      proc: ProcedureInfo,
      steps: List[str],
      counter: List[int],
      visited: set,
  ) -> None:
    """
    Recursive helper that appends narrative steps for a procedure AND its children.
    `counter` is a single‑element list used to keep a running step number across recursion.
    """
    code = proc.definition

    def add_step(text: str) -> None:
      steps.append(f"{counter[0]}. {text}")
      counter[0] += 1

    # High‑level narrative for this procedure
    summary_sentence = self._summarize_actions_sentence(proc)
    if summary_sentence:
      add_step(summary_sentence)

    # More specific hints based on patterns
    if "SET NOCOUNT ON" in code.upper():
      add_step(
        f"While running **{proc.schema}.{proc.name}**, it prepares the environment so that only meaningful results "
        "are returned, avoiding extra technical messages."
      )

    if "DECLARE" in code.upper():
      add_step(
        f"**{proc.schema}.{proc.name}** sets up internal placeholders (variables) to keep track of things like "
        "billing periods, status flags, or messages."
      )

    if re.search(r"\bBillingPeriod\b", code, re.IGNORECASE):
      add_step(
        f"**{proc.schema}.{proc.name}** figures out which billing period it should work on – usually the most recent "
        "or currently active one."
      )

    if self.INSERT_PATTERN.search(code):
      if "History" in code or "Archive" in code:
        add_step(
          f"**{proc.schema}.{proc.name}** copies existing data into a history or archive area so that past information "
          "is preserved before new work is done."
        )
      elif "Log" in code or "Usage" in code:
        add_step(
          f"**{proc.schema}.{proc.name}** writes a log entry summarizing what report or process was requested, by whom, "
          "and with which key settings."
        )
      else:
        add_step(
          f"**{proc.schema}.{proc.name}** adds new records into one or more tables, reflecting the latest information "
          "received from external files or systems."
        )

    if self.TRUNCATE_PATTERN.search(code):
      add_step(
        f"**{proc.schema}.{proc.name}** completely clears temporary or staging tables so that the next run starts with "
        "a clean slate and no leftover data."
      )

    if "IsSuccess" in code:
      add_step(
        f"**{proc.schema}.{proc.name}** marks whether this step finished successfully or ran into a problem, so that "
        "progress can be tracked later."
      )

    if self.TRY_PATTERN.search(code) or self.CATCH_PATTERN.search(code):
      add_step(
        f"**{proc.schema}.{proc.name}** has its own error handling: if anything goes wrong, it captures the details and "
        "records that the step failed."
      )

    # Now walk its children recursively
    children = self.find_called_procedures(proc)
    for full_child_name in children:
      if full_child_name in visited:
        # Avoid infinite loops in pathological call graphs
        add_step(
          f"**{proc.schema}.{proc.name}** also calls **{full_child_name}**, but that procedure has already been "
          "described earlier in this flow."
        )
        continue

      child_proc = self._lookup_procedure_by_full_name(full_child_name)
      if not child_proc:
        add_step(
          f"**{proc.schema}.{proc.name}** hands off a portion of the work to another stored procedure "
          f"(**{full_child_name}**), whose details are not available in this export."
        )
        continue

      visited.add(full_child_name)
      add_step(
        f"Next, **{proc.schema}.{proc.name}** calls **{child_proc.schema}.{child_proc.name}** to handle a more "
        "specialized part of the process."
      )

      # Recurse into the child so its logic becomes part of the same narrative
      self._append_steps_for_proc(child_proc, steps, counter, visited)

  def generate_step_by_step_flow(self, root_proc: ProcedureInfo) -> List[str]:
    """
    Produce a simplified, non-technical “step-by-step” description that covers
    the complete call tree starting from `root_proc`.
    """
    steps: List[str] = []
    counter = [1]
    visited: set = {f"{root_proc.schema}.{root_proc.name}"}

    self._append_steps_for_proc(root_proc, steps, counter, visited)

    # Final overall step
    steps.append(
      f"{counter[0]}. Once all of these procedures have finished, the system is left in a consistent state so that "
      "later steps or reports can rely on the updated data."
    )
    return steps

  # -----------------------------
  # Recursive call graph builder
  # -----------------------------

  def _collect_call_graph(
      self,
      proc: ProcedureInfo,
      edges: List[Tuple[str, str]],
      nodes: Dict[str, ProcedureInfo],
      visited: set,
  ) -> None:
    """
    Recursive helper to collect the full call graph starting at `proc`.
    Fills:
      - nodes: full_name -> ProcedureInfo
      - edges: (caller_full_name, callee_full_name)
    """
    full_name = f"{proc.schema}.{proc.name}"
    nodes[full_name] = proc

    children = self.find_called_procedures(proc)
    for full_child_name in children:
      edges.append((full_name, full_child_name))

      if full_child_name in visited:
        continue

      child_proc = self._lookup_procedure_by_full_name(full_child_name)
      if not child_proc:
        continue

      visited.add(full_child_name)
      self._collect_call_graph(child_proc, edges, nodes, visited)

  def _node_id_from_full_name(self, full_name: str) -> str:
    """
    Turn 'Schema.ProcName' into a safe Mermaid node id.
    """
    return "N_" + re.sub(r"[^0-9A-Za-z_]", "_", full_name)

  def generate_mermaid_flowchart(
      self,
      root_proc: ProcedureInfo,
  ) -> str:
    """
    Build a Mermaid flowchart for the complete call tree starting at `root_proc`.

    Each procedure in the tree is shown as its own node, with a short label
    describing what it does. Edges represent “calls” (who calls whom).
    """
    root_full = f"{root_proc.schema}.{root_proc.name}"

    edges: List[Tuple[str, str]] = []
    nodes: Dict[str, ProcedureInfo] = {}
    visited: set = {root_full}

    self._collect_call_graph(root_proc, edges, nodes, visited)

    mermaid_lines = [
      "```mermaid",
      "flowchart TD",
      "    START([Start]) --> ROOT",
    ]

    # Define all nodes with short descriptions
    for full_name, proc in nodes.items():
      label_name = full_name
      summary = self._summarize_actions_sentence(proc) or ""
      # Keep label readable: procedure name on first line, short description on second line
      if summary:
        short = summary.replace("Inside", "").replace("it ", "").strip()
        node_label = f"{label_name}\\n{short}"
      else:
        node_label = label_name

      node_id = "ROOT" if full_name == root_full else self._node_id_from_full_name(full_name)
      mermaid_lines.append(f"    {node_id}[[{node_label}]]")

    # Add edges for calls
    for caller, callee in edges:
      caller_id = "ROOT" if caller == root_full else self._node_id_from_full_name(caller)
      callee_id = "ROOT" if callee == root_full else self._node_id_from_full_name(callee)
      mermaid_lines.append(f"    {caller_id} --> {callee_id}")

    # Connect root to end
    mermaid_lines.append("    ROOT --> END([End])")
    mermaid_lines.append("```")

    return "\n".join(mermaid_lines)

  # -----------------------------
  # Orchestration
  # -----------------------------

  def build_documentation_for_procedure(self, proc: ProcedureInfo) -> ProcedureFlowDocumentation:
    # Direct children (first-level calls); their own children will be pulled
    # into the diagram and narrative by the recursive helpers.
    called_procs = self.find_called_procedures(proc)

    summary = self.generate_high_level_summary(proc, called_procs)
    steps = self.generate_step_by_step_flow(proc)
    mermaid = self.generate_mermaid_flowchart(proc)

    title = f"{proc.schema}.{proc.name}"

    return ProcedureFlowDocumentation(
      schema=proc.schema,
      name=proc.name,
      title=title,
      mermaid_flowchart=mermaid,
      high_level_summary=summary,
      step_by_step_flow=steps,
      called_procedures=called_procs,
    )

  def render_markdown(self, docs: List[ProcedureFlowDocumentation]) -> str:
    lines: List[str] = []
    lines.append("# Stored Procedure Flow & Logic (Non‑Technical Overview)")
    lines.append("")
    lines.append(
      "This document describes each selected stored procedure and its complete call tree. "
      "The goal is to show, in plain language, how the main procedure and all the procedures it calls "
      "work together to carry out a full business workflow."
    )
    lines.append("")

    for doc in docs:
      lines.append("")
      lines.append(f"## {doc.title}")
      lines.append("")
      lines.append("### 1. Big‑Picture Explanation")
      lines.append("")
      lines.append(doc.high_level_summary)
      lines.append("")
      lines.append("### 2. Flow Diagram (Complete Call Tree)")
      lines.append("")
      lines.append(doc.mermaid_flowchart)
      lines.append("")
      lines.append("### 3. Step‑by‑Step Narrative (Complete Call Tree)")
      lines.append("")
      for step in doc.step_by_step_flow:
        lines.append(f"- {step}")
      lines.append("")
      # No separate “Other Procedures Involved” section anymore; their logic
      # is integrated into the diagram and narrative above.
      lines.append("---")

    return "\n".join(lines)

  def save_markdown(self, content: str) -> None:
    os.makedirs(os.path.dirname(self.output_markdown_path), exist_ok=True)
    with open(self.output_markdown_path, "w", encoding="utf-8") as f:
      f.write(content)
    logger.info("Flow & logic documentation written to: %s", self.output_markdown_path)

  def run(self) -> None:
    """Main entry point for generating the flow documentation."""
    logger.info("Starting StoredProcedureFlowDocumenter run()")

    self.load_all_procedures()
    to_document = self.load_procedures_to_process()

    if not to_document:
      logger.warning("No procedures resolved to document. Nothing to do.")
      return

    docs: List[ProcedureFlowDocumentation] = []
    for proc in to_document:
      logger.info("Documenting procedure flow: %s.%s", proc.schema, proc.name)
      doc = self.build_documentation_for_procedure(proc)
      docs.append(doc)

    markdown = self.render_markdown(docs)
    self.save_markdown(markdown)


# -----------------------------
# CLI Entry Point
# -----------------------------

def main():
  """
  Command‑line entry point.

  Usage (from the directory containing this file and the export folder):
      python StoredProcedureFlowDocumentationGenerator.py

  Assumes the following files exist in the /export folder next to this script:
    - stored_procedures_analysis_all_schemas.json
    - stored_procedures_to_process_logic.json

  Outputs:
    - /export/stored_procedures_logic_flows.md
  """
  documenter = StoredProcedureFlowDocumenter()
  try:
    documenter.run()
  except Exception as exc:
    logger.error("StoredProcedureFlowDocumenter failed: %s", exc)
    raise


if __name__ == "__main__":
  main()
