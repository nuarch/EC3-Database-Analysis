# Execution flow for schema 'Scheduler'

This document explains, in everyday language, how the stored procedures that appear in the related diagram can trigger one another when they run.

## Overview

The following procedures act as **starting points** in this diagram. They can be run directly and then may trigger other procedures shown:

- procedure 'usp_ArchiveReport' in schema 'Scheduler'

## Detailed call paths

Below is a step-by-step description of how each procedure in the diagram can lead to others. Nested bullet points show what can be triggered next.

- procedure 'usp_ArchiveReport' in schema 'Scheduler' can trigger:
  - procedure 'usp_InsertDocumentEndOfMonth' in schema 'ContentManagement' (in this diagram, this procedure does not trigger any other recorded procedures)