## Phase 4: Gold Dimensional Model

After completing Phase 3, build the Power BI-ready star schema:

```powershell
python notebooks/05_gold_dimensional_model.py
python -m pytest
```

The Gold layer contains conformed property, tenant, and date dimensions plus lease, rent payment, maintenance, and property budget facts. It implements deterministic surrogate keys, unknown members, declared fact grains, dimensional lookups, and reusable financial measures.

Local outputs are written to `data/gold/`. The Fabric notebook writes managed Delta tables to the `gold` schema for Direct Lake consumption.
