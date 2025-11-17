# Execution flow for schema 'Published'

This document explains, in everyday language, how the stored procedures that appear in the related diagram can trigger one another when they run.

## Overview

The following procedures act as **starting points** in this diagram. They can be run directly and then may trigger other procedures shown:

- procedure 'usp_AfterPublishingDashboardAndReportTablesRefresh' in schema 'Published'
- procedure 'usp_Agency_Overview' in schema 'Published'
- procedure 'usp_CreatePublishedTablesFromViewsForCurrentBillingPeriod' in schema 'Published'
- procedure 'usp_Facility_Overview' in schema 'Published'
- procedure 'usp_GlobalSearch' in schema 'Published'

## Detailed call paths

Below is a step-by-step description of how each procedure in the diagram can lead to others. Nested bullet points show what can be triggered next.

- procedure 'usp_AfterPublishingDashboardAndReportTablesRefresh' in schema 'Published' can trigger:
  - procedure 'usp_Load_EstimatedActualCounts' in schema 'Billing' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_Agency_Overview' in schema 'Published' can trigger:
  - procedure 'usp_AddReportUsageLog' in schema 'Audit' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_CreatePublishedTablesFromViewsForCurrentBillingPeriod' in schema 'Published' can trigger:
  - procedure 'usp_CreateAccountLevelSummaryByDollarsBtusCo2AndEnergyType' in schema 'Published' (in this diagram, this procedure does not trigger any other recorded procedures)
  - procedure 'usp_Create_EnergyUsageSummaryGroupByAgencyAndEnergyType' in schema 'Published' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_Facility_Overview' in schema 'Published' can trigger:
  - procedure 'usp_AddReportUsageLog' in schema 'Audit' (in this diagram, this procedure does not trigger any other recorded procedures)
- procedure 'usp_GlobalSearch' in schema 'Published' can trigger:
  - procedure 'usp_AddReportUsageLog' in schema 'Audit' (in this diagram, this procedure does not trigger any other recorded procedures)