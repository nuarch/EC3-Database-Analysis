# Execution flow for schema 'NationalGridWest'

This document explains, in everyday language, how the stored procedures that appear in the related diagram can trigger one another when they run.

## Overview

The following procedures act as **starting points** in this diagram. They can be run directly and then may trigger other procedures shown:

- procedure 'usp_LegacyData_ProcessAccountCancellationSpanned' in schema 'NationalGridWest'
- procedure 'usp_LegacyData_ProcessAccountSpanned' in schema 'NationalGridWest'
- procedure 'usp_LegacyData_ProcessMeterCancellationSpanned' in schema 'NationalGridWest'
- procedure 'usp_LegacyData_ProcessMeterSpanned' in schema 'NationalGridWest'
- procedure 'usp_LegacyData_ProcessMeterUnspanned' in schema 'NationalGridWest'

## Detailed call paths

Below is a step-by-step description of how each procedure in the diagram can lead to others. Nested bullet points show what can be triggered next.

- procedure 'usp_LegacyData_ProcessAccountCancellationSpanned' in schema 'NationalGridWest' can trigger:
  - procedure 'usp_LegacyData_ParseSpannedAccountCancellationInfo' in schema 'NationalGridWest' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_LegacyData_ProcessAccountSpanned' in schema 'NationalGridWest' can trigger:
  - procedure 'usp_LegacyData_ParseSpannedAccountInfo' in schema 'NationalGridWest' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_LegacyData_ProcessMeterCancellationSpanned' in schema 'NationalGridWest' can trigger:
  - procedure 'usp_LegacyData_ParseSpannedMeterCancellationInfo' in schema 'NationalGridWest' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_LegacyData_ProcessMeterSpanned' in schema 'NationalGridWest' can trigger:
  - procedure 'usp_LegacyData_ParseSpannedMeterInfo' in schema 'NationalGridWest' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_LegacyData_ProcessMeterUnspanned' in schema 'NationalGridWest' can trigger:
  - procedure 'usp_LegacyData_ParseSplitBillingAndMeterResets' in schema 'NationalGridWest' (in this diagram, this procedure does not trigger any other recorded procedures)

## Additional procedures
The following procedures are also shown in the diagram but mainly appear in the middle of call chains:
- procedure 'usp_LegacyData_Main_ProcessLegacyNationalGridData' in schema 'NationalGridWest' can trigger:
  - procedure 'usp_LegacyData_Main_ProcessLegacyNationalGridData' in schema 'NationalGridWest' (this procedure can eventually call itself again through a loop of calls shown in the diagram)