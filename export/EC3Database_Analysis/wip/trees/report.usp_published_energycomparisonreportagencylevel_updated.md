## Execution Tree — report.usp_published_energycomparisonreportagencylevel_updated

```mermaid
graph TD
    audit_usp_addreportusagelog[audit.usp_addreportusagelog]
    report_usp_report_getcustomagencylist[report.usp_report_getcustomagencylist]
    report_usp_published_energycomparisonreportagencylevel_updated[report.usp_published_energycomparisonreportagencylevel_updated] --> audit_usp_addreportusagelog[audit.usp_addreportusagelog]
    report_usp_published_energycomparisonreportagencylevel_updated[report.usp_published_energycomparisonreportagencylevel_updated] --> report_usp_report_getcustomagencylist[report.usp_report_getcustomagencylist]
```
