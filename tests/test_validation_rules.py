from src.validation_utils import validate_required_fields, find_duplicate_keys


def test_validate_required_fields_returns_errors():
    record = {"property_id": 101, "property_name": ""}
    errors = validate_required_fields(record, ["property_id", "property_name"])
    assert len(errors) == 1


def test_find_duplicate_keys():
    records = [{"id": 1}, {"id": 2}, {"id": 1}]
    duplicates = find_duplicate_keys(records, "id")
    assert duplicates == {1}
