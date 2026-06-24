## Phase 5: Power BI Semantic Model

Validate the Gold layer against the version-controlled semantic model:

```powershell
python notebooks/06_validate_semantic_model.py
python -m pytest
```

The Power BI assets define a Direct Lake semantic model, conformed relationships, governed DAX measures, formatting, security guidance, and six report pages. Follow `powerbi/semantic-model/build-guide.md` to implement the model in Fabric.
