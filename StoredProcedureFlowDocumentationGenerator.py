import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# -----------------------------
# ChatGPT configuration
# -----------------------------

def load_chatgpt_config() -> Dict[str, Any]:
  """
  Load ChatGPT configuration from an external file (chatgpt_config.py) or
  from environment variables.

  Expected keys:
    - api_key
    - base_url
    - model
    - timeout
    - max_retries
    - max_tokens
    - temperature
  """
  try:
    from chatgpt_config import CHATGPT_CONFIG  # type: ignore
    return CHATGPT_CONFIG
  except ImportError:
    logger.warning("chatgpt_config.py not found, using environment variables for ChatGPT config")
    return {
      "api_key": os.getenv("OPENAI_API_KEY", ""),
      "base_url": os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
      "model": os.getenv("OPENAI_MODEL", "gpt-4o"),
      "timeout": int(os.getenv("OPENAI_TIMEOUT", "60")),
      "max_retries": int(os.getenv("OPENAI_MAX_RETRIES", "3")),
      "max_tokens": int(os.getenv("OPENAI_MAX_TOKENS", "4000")),
      "temperature": float(os.getenv("OPENAI_TEMPERATURE", "0.1")),
    }


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
  """
  Output for one root procedure.

  All human‑readable content (overview, flow diagram, step‑by‑step narrative)
  is contained in markdown_section, which is generated entirely by ChatGPT.
  """
  schema: str
  name: str
  title: str
  markdown_section: str


# -----------------------------
# Main Documenter Class
# -----------------------------

class StoredProcedureFlowDocumenter:
  """
  Reads:
    1) /export/stored_procedures_analysis_all_schemas.json
    2) /export/stored_procedures_to_process_logic.json

  And produces:
    - A Markdown document where *ChatGPT* generates, for each selected
      stored procedure, a non‑technical explanation of the complete call tree
      (the procedure and any other procedures it calls), including:

        * Overview in human, non‑technical language
        * Flow diagram (Mermaid)
        * Step‑by‑step narrative

  This class does NOT attempt to parse SQL itself. All logic extraction and
  diagram generation is delegated to ChatGPT.
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

    # ChatGPT config & HTTP session
    cfg = load_chatgpt_config()
    self.api_key: str = cfg.get("api_key", "")
    self.base_url: str = cfg.get("base_url", "https://api.openai.com/v1")
    self.model: str = cfg.get("model", "gpt-4o")
    self.timeout: int = cfg.get("timeout", 60)
    self.max_retries: int = cfg.get("max_retries", 3)
    self.max_tokens: int = cfg.get("max_tokens", 4000)
    self.temperature: float = cfg.get("temperature", 0.1)

    self.session = requests.Session()
    if self.api_key:
      self.session.headers.update(
        {
          "Authorization": f"Bearer {self.api_key}",
          "Content-Type": "application/json",
        }
      )
      logger.info("ChatGPT API key loaded successfully")
    else:
      logger.warning("No ChatGPT API key found – the documenter will not be able to call the API")

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
    Load the list of root procedures whose logic/flow we should document.

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
  # ChatGPT Prompt & Call
  # -----------------------------

  def _build_call_tree_prompt(self, proc: ProcedureInfo) -> str:
    """
    Build a prompt that gives ChatGPT everything it needs to:
      - Inspect the SQL for one root stored procedure
      - Detect any other procedures it calls (by reading the SQL)
      - Reason about those calls conceptually
      - Produce a non‑technical explanation, flow diagram, and narrative
    """
    return f"""
You are a senior data engineer documenting a Microsoft SQL Server system for non‑technical stakeholders.

You are given the full SQL definition of ONE root stored procedure. Your job is to:

1. Read this stored procedure's SQL.
2. From the SQL alone, determine which OTHER stored procedures it calls (if any).
3. Conceptually treat **the root procedure plus any called procedures** as a single end‑to‑end workflow.
4. Produce documentation that explains the complete call chain in clear, non‑technical language.

Root stored procedure (schema and name):
- {proc.schema}.{proc.name}

SQL definition of the root stored procedure:
  
```sql
{proc.definition}
```

Important instructions:

- You MUST infer called procedures by looking for EXEC / EXECUTE statements or similar in the SQL.
- You SHOULD treat the root procedure and all called procedures as a unified flow when explaining the logic.
- Use business‑oriented, human language. Avoid deep SQL jargon.
- Explain the **overall purpose**, the **major steps**, and how control flows between procedures.
- When you mention other procedures, use their full names as they appear in the SQL.

Output format (Markdown):

### 1. Overview

- A few paragraphs in plain English explaining:
  - The overall purpose of the root procedure.
  - How it fits into a larger process (e.g., data loading, validation, reporting).
  - The role of any other procedures it calls.

### 2. Flow Diagram (Mermaid)

Produce a single Mermaid **flowchart** that shows the complete call tree and the main phases of work.

- Use this structure:
  
mermaid flowchart TD START([Start]) --> ROOT[Schema.RootProcedureName] %% Add nodes for important steps inside the root procedure %% Add nodes for any other procedures that are called %% Connect them so the call flow is clear %% End with an END node
  

- Use simple, human‑readable labels (short phrases like “Archive raw data”, “Clear staging tables”, “Log usage”, etc.).
- Make sure the diagram clearly shows which procedure calls which.

### 3. Step-by-Step Narrative (Complete Call Tree)

Write a numbered list that walks through the entire flow in order, for example:

1. What the root procedure does first.
2. When and why it calls another procedure, and what that procedure does in business terms.
3. What happens after each call returns.
4. How the overall process finishes and what state the data ends up in.

Guidelines:

- Keep the language **non‑technical** and focused on business meaning.
- Do NOT describe things in terms of “loops”, “cursors”, “joins”, etc.; instead say what is happening in business terms (e.g., “it looks up the current billing period”, “it copies billing records into a history area”).
- Assume the reader understands what a “stored procedure” is at a high level, but not SQL syntax.

Only produce the Markdown content requested above (no extra commentary).
"""

  def call_chatgpt_for_procedure(self, proc: ProcedureInfo) -> str:
    """
    Call ChatGPT to generate the full markdown section for a single root procedure.
    """
    if not self.api_key:
      raise RuntimeError(
        "ChatGPT API key is not configured. Set OPENAI_API_KEY or provide chatgpt_config.py."
      )

    prompt = self._build_call_tree_prompt(proc)

    payload = {
      "model": self.model,
      "messages": [
        {
          "role": "system",
          "content": (
            "You are an expert SQL and data engineering documentation assistant. "
            "You explain complex stored procedure workflows in simple, non‑technical language."
          ),
        },
        {
          "role": "user",
          "content": prompt,
        },
      ],
      "max_tokens": self.max_tokens,
      "temperature": self.temperature,
    }

    for attempt in range(self.max_retries):
      try:
        response = self.session.post(
          f"{self.base_url}/chat/completions",
          json=payload,
          timeout=self.timeout,
        )

        if response.status_code == 200:
          data = response.json()
          content = data["choices"][0]["message"]["content"]
          logger.info("Got ChatGPT documentation for %s.%s", proc.schema, proc.name)
          return content
        else:
          logger.error(
            "ChatGPT API request failed (status %s): %s",
            response.status_code,
            response.text,
          )
          if attempt < self.max_retries - 1:
            continue
          raise RuntimeError(
            f"ChatGPT API request failed after {self.max_retries} attempts "
            f"for {proc.schema}.{proc.name}"
          )
      except requests.RequestException as exc:
        logger.error(
          "ChatGPT API request error for %s.%s (attempt %d/%d): %s",
          proc.schema,
          proc.name,
          attempt + 1,
          self.max_retries,
          exc,
          )
        if attempt < self.max_retries - 1:
          continue
        raise

    # Should never reach here
    raise RuntimeError(f"Unexpected error calling ChatGPT for {proc.schema}.{proc.name}")

  # -----------------------------
  # Orchestration
  # -----------------------------

  def build_documentation_for_procedure(self, proc: ProcedureInfo) -> ProcedureFlowDocumentation:
    """
    For a single root procedure, delegate all analysis and formatting to ChatGPT.
    """
    markdown_section = self.call_chatgpt_for_procedure(proc)
    title = f"{proc.schema}.{proc.name}"

    return ProcedureFlowDocumentation(
      schema=proc.schema,
      name=proc.name,
      title=title,
      markdown_section=markdown_section,
    )

  def render_markdown(self, docs: List[ProcedureFlowDocumentation]) -> str:
    """
    Render a single Markdown document that simply wraps the sections
    produced by ChatGPT for each procedure.
    """
    lines: List[str] = []
    lines.append("# Stored Procedure Flow & Logic (Non‑Technical Overview)")
    lines.append("")
    lines.append(
      "This document was generated by asking an AI assistant (ChatGPT) to read each stored procedure's SQL "
      "and explain, in non‑technical language, what it does and how any other procedures it calls fit "
      "into the overall flow."
    )
    lines.append("")

    for doc in docs:
      lines.append("")
      lines.append(f"## {doc.title}")
      lines.append("")
      lines.append(doc.markdown_section.strip())
      lines.append("")
      lines.append("---")

    return "\n".join(lines)

  def save_markdown(self, content: str) -> None:
    os.makedirs(os.path.dirname(self.output_markdown_path), exist_ok=True)
    with open(self.output_markdown_path, "w", encoding="utf-8") as f:
      f.write(content)
    logger.info("Flow & logic documentation written to: %s", self.output_markdown_path)

  def run(self) -> None:
    """Main entry point for generating the flow documentation."""
    logger.info("Starting StoredProcedureFlowDocumenter.run()")

    self.load_all_procedures()
    to_document = self.load_procedures_to_process()

    if not to_document:
      logger.warning("No procedures resolved to document. Nothing to do.")
      return

    docs: List[ProcedureFlowDocumentation] = []
    for proc in to_document:
      logger.info("Documenting procedure flow via ChatGPT: %s.%s", proc.schema, proc.name)
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

  Also assumes ChatGPT configuration is available via:
    - chatgpt_config.py (CHATGPT_CONFIG dict), or
    - environment variables (OPENAI_API_KEY, OPENAI_BASE_URL, etc.).

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
