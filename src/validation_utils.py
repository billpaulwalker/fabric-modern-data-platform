from typing import Iterable


def validate_required_fields(record: dict, required_fields: Iterable[str]) -> list[str]:
    errors = []
    for field in required_fields:
        value = record.get(field)
        if value is None or value == "":
            errors.append(f"Missing required field: {field}")
    return errors


def find_duplicate_keys(records: list[dict], key_field: str) -> set:
    seen = set()
    duplicates = set()

    for record in records:
        key = record.get(key_field)
        if key in seen:
            duplicates.add(key)
        else:
            seen.add(key)

    return duplicates


def split_valid_invalid_records(records: list[dict], required_fields: Iterable[str]) -> tuple[list[dict], list[dict]]:
    valid_records = []
    invalid_records = []

    for record in records:
        errors = validate_required_fields(record, required_fields)
        if errors:
            rejected = dict(record)
            rejected["_validation_errors"] = errors
            invalid_records.append(rejected)
        else:
            valid_records.append(record)

    return valid_records, invalid_records
