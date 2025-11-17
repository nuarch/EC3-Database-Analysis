# Execution flow for schema 'ConEd'

This document explains, in everyday language, how the stored procedures that appear in the related diagram can trigger one another when they run.

## Overview

The following procedures act as **starting points** in this diagram. They can be run directly and then may trigger other procedures shown:

- procedure 'Archive_usp_ProcessConEdTransformMeterBillingData' in schema 'ConEd'
- procedure 'usp_Upload_18_ProcessExchange' in schema 'ConEd'
- procedure 'usp_Upload_19_UploadDataToEC3' in schema 'ConEd'
- procedure 'usp_Upload_2_TransformRawData' in schema 'ConEd'

## Detailed call paths

Below is a step-by-step description of how each procedure in the diagram can lead to others. Nested bullet points show what can be triggered next.

- procedure 'Archive_usp_ProcessConEdTransformMeterBillingData' in schema 'ConEd' can trigger:
  - procedure 'Archive_usp_ParseSpannedBilledUploadConEdisonMeterInfo' in schema 'ConEd' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'Archive_usp_ParseSplitBillingAndMeterResetsUploadConEdisonMeterInfo' in schema 'ConEd' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_Upload_18_ProcessExchange' in schema 'ConEd' can trigger:
  - procedure 'usp_Upload_18_Helper_ProcessCommonElectronicGasUnknownExchangeCode' in schema 'ConEd' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_Upload_19_UploadDataToEC3' in schema 'ConEd' can trigger:
  - procedure 'usp_Upload_19_helper_FixServiceClassificationDiscrepancy' in schema 'ConEd' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_Upload_19_helper_UploadAccountDataToEC3' in schema 'ConEd' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_Upload_19_helper_UploadMeterDataToEC3' in schema 'ConEd' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_Upload_2_TransformRawData' in schema 'ConEd' can trigger:
  - procedure 'CopyConEdExchangeDataToCommon' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'CopyConEdExchangeDataUploadToCommon' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_CONED_HandleExchangeDataIssues' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_ProcessExchangeData' in schema 'Common' (in this diagram, this procedure does not trigger any other recorded procedures)