# Phase 5: Power BI Semantic Model

## Objective

Turn the Gold star schema into a governed Power BI semantic model with explicit relationships, reusable DAX measures, consistent formatting, a report design, and automated pre-deployment checks.

## Architecture

```mermaid
flowchart TD
    G["Gold Delta tables"] --> S["Direct Lake semantic model"]
    S --> M["Governed DAX measures"]
    M --> R["CRE portfolio report"]
    S --> V["Semantic validation"]
```

## Deliverables

- Version-controlled semantic-model contract
- Twelve one-to-many relationship definitions
- Active and inactive role-playing date relationships
- Governed DAX measure library
- Measure formatting catalog
- Six-page report design
- Power BI theme
- Direct Lake decision record
- Security design notes
- Local semantic validation script and unit tests
- SQL reconciliation queries

## Senior-Level Practices

**Thin semantic model:** Transformations and reusable row-level calculations live in Gold; DAX handles filter-context-dependent aggregations.

**Explicit measure ownership:** Report authors use governed measures instead of implicit column sums.

**Single-direction star relationships:** Dimensions filter facts without ambiguous bidirectional paths.

**Role-playing dates:** Primary date relationships remain active; secondary dates use inactive relationships and `USERELATIONSHIP` measures.

**Deployment gate:** The model contract is validated against the physical Gold outputs before publication.

## Local Run

Run Phase 4 first, then:

```powershell
python notebooks/06_validate_semantic_model.py
python -m pytest
```

Review:

```text
data/gold/semantic_model_validation.json
```

The validation fails when a fact's measure columns are all zero or null (`non_empty_columns`), when an active relationship has null foreign keys, or when more than `max_unknown_member_ratio` of a fact's rows resolve to the Unknown member.

## Fabric Execution

1. Run the Gold notebook so the `gold` schema tables are current.
2. Upload `config/semantic_model_config.json` to the Lakehouse at `Files/config/`.
3. Create a notebook from `notebooks/fabric/06_validate_semantic_model_pyspark.py` and attach the Lakehouse.
4. Run all cells. The notebook applies the same contract as the local validation to the `gold` tables and raises an error if any check fails, which stops the pipeline before the semantic model is refreshed.
5. DAX measure names are checked in repository CI, where `powerbi/semantic-model/measures.dax` lives.

## Completion Checkpoint

- Semantic validation returns `passed: true`.
- Seven Gold tables are present in the Direct Lake model.
- All relationships match the configuration.
- The date table and sort properties are configured.
- Required DAX measures exist and match SQL spot checks.
- Technical fields are hidden and measures are formatted.
- Report pages render correctly under common filters.
- Model and report are saved in the Development workspace.
