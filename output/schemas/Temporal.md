# Execution flow for schema 'Temporal'

This document explains, in everyday language, how the stored procedures that appear in the related diagram can trigger one another when they run.

## Overview

The following procedures act as **starting points** in this diagram. They can be run directly and then may trigger other procedures shown:

- procedure 'usp_ProcessAndInsertTemporalData' in schema 'Temporal'

## Detailed call paths

Below is a step-by-step description of how each procedure in the diagram can lead to others. Nested bullet points show what can be triggered next.

- procedure 'usp_ProcessAndInsertTemporalData' in schema 'Temporal' can trigger:
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalAccountLevelSummaryByAgency' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalAccountLevelSummaryByCityWide' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalAccountLevelSummaryByFacility' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalAccountMeterLevelRawDataForCurrentPeriod' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalEnergyUsageSummaryGroupByAgencyAndEnergyType' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalFiscalYearPivotByAgencyAndFacilityDollarsAndUsage' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalFiscalYearPivotByAgencyDollarsAndUsage' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalFiscalYearPivotByAgencyFacilityAndAccountDollarsAndUsage' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalFiscalYearPivotByEncoreMonthlyPayments' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalForAccountLevelRawDataForCurrentPeriod' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_InsertNewPeriodFromPublishedToTemporalForAccountLevelSummaryForDollarsBtusAndCo2' in schema 'Temporal' (in this diagram, this procedure does not trigger any other recorded procedures)