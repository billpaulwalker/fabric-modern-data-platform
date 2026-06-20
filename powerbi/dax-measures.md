# DAX Measures

## Rent Collection

```DAX
Total Rent Collected =
SUM(fact_rent_payment[payment_amount])
```

```DAX
Total Rent Billed =
SUM(fact_rent_payment[amount_billed])
```

```DAX
Collection Rate =
DIVIDE(
    [Total Rent Collected],
    [Total Rent Billed]
)
```

```DAX
Late Payment Count =
CALCULATE(
    COUNTROWS(fact_rent_payment),
    fact_rent_payment[payment_status] = "Late"
)
```

## Occupancy

```DAX
Occupied Square Feet =
SUM(fact_lease[leased_square_feet])
```

```DAX
Occupancy Rate =
DIVIDE(
    [Occupied Square Feet],
    SUM(dim_property[square_feet])
)
```
