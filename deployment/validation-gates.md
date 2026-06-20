# Validation Gates

## Gate 1: Repository Validation

- Required folders exist
- README exists
- Python files compile
- SQL files exist
- No `.env` file committed

## Gate 2: Data Validation

- Required fields populated
- No duplicate primary keys
- Row counts are reasonable
- Rejected records captured

## Gate 3: Model Validation

- Gold tables have expected columns
- Fact tables join to dimensions
- Measures return expected results
- Report pages load successfully
