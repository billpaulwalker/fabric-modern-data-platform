# ADF-Style Orchestration Pattern

## Pattern

```text
Lookup active source config
  ↓
ForEach active source
  ↓
Set parameters
  ↓
Run ingestion notebook
  ↓
Capture row counts
  ↓
Update logging table
  ↓
Update watermark on success
```

## Why This Matters

This demonstrates metadata-driven orchestration and reusable pipeline design instead of creating one hardcoded pipeline per source.
