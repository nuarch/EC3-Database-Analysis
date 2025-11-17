# Execution flow for schema 'Common'

This document explains, in everyday language, how the stored procedures that appear in the related diagram can trigger one another when they run.

## Overview

The following procedures act as **starting points** in this diagram. They can be run directly and then may trigger other procedures shown:

- procedure 'usp_CONED_HandleExchangeDataIssues' in schema 'Common'
- procedure 'usp_ProcessExchangeData' in schema 'Common'

## Detailed call paths

Below is a step-by-step description of how each procedure in the diagram can lead to others. Nested bullet points show what can be triggered next.

- procedure 'usp_CONED_HandleExchangeDataIssues' in schema 'Common' can trigger:
  - procedure 'CopyConEdExchangeDataToCommon' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_ProcessExchangeData' in schema 'Common' can trigger:
  - procedure 'usp_ProcessExchange_Code27' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_ProcessExchange_Code28' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_ProcessExchange_Code45' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_ProcessExchange_Code46' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_ProcessExchange_Code47' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_ProcessExchange_CodeAX' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_ProcessExchange_CodeMISC' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)