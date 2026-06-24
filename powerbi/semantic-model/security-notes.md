# Semantic Model Security Notes

The portfolio model does not invent user-to-region authorization data. Production row-level security requires a governed security mapping such as:

```text
user_principal_name | property_key | region | effective_from | effective_to
```

Recommended implementation:

1. Ingest the approved access mapping through the medallion layers.
2. Publish a Gold security bridge with one row per authorized user and property.
3. Relate the bridge to `dim_property` using a tested security pattern.
4. Filter by `USERPRINCIPALNAME()` in the semantic-model role.
5. Test allowed and denied scenarios using **View as role**.
6. Manage workspace roles separately from data-level RLS.

Do not hard-code employee email addresses or treat workspace access as a substitute for row-level authorization.
