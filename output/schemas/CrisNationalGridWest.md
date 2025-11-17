# Execution flow for schema 'CrisNationalGridWest'

This document explains, in everyday language, how the stored procedures that appear in the related diagram can trigger one another when they run.

## Overview

The following procedures act as **starting points** in this diagram. They can be run directly and then may trigger other procedures shown:

- procedure 'usp_CRIS_SpanParseAndMergeCancellationAccountBillingAdjustmentRecords' in schema 'CrisNationalGridWest'
- procedure 'usp_CRIS_ValidateAccountTransactions' in schema 'CrisNationalGridWest'
- procedure 'usp_ProcessExchangeInfo' in schema 'CrisNationalGridWest'

## Detailed call paths

Below is a step-by-step description of how each procedure in the diagram can lead to others. Nested bullet points show what can be triggered next.

- procedure 'usp_CRIS_SpanParseAndMergeCancellationAccountBillingAdjustmentRecords' in schema 'CrisNationalGridWest' can trigger:
  - procedure 'usp_CRIS_PreProcessParsingSpannedAccountInfoCancellation' in schema 'CrisNationalGridWest' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_CRIS_ValidateAccountTransactions' in schema 'CrisNationalGridWest' can trigger:
  - procedure 'usp_CRIS_CorrectPartialCancelattionFromPriorPeriodMergedBill' in schema 'CrisNationalGridWest' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_ProcessExchangeInfo' in schema 'CrisNationalGridWest' can trigger:
  - procedure 'usp_ProcessExchangeCode_27' in schema 'CrisNationalGridWest' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_ProcessExchangeCode_28' in schema 'CrisNationalGridWest' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_ProcessExchangeCode_MISC' in schema 'CrisNationalGridWest' (in this diagram, this procedure does not trigger any other recorded procedures)