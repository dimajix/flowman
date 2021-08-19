# Unique Key Assertion

## Example:

```yaml
kind: uniqueKey
mapping: some_mapping
key: id
```

```yaml
kind: uniqueKey
mapping: exchange_rates
key:
  - date
  - from_currency
  - to_currency 
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `uniqueKey`

* `description` **(optional)** *(type: string)*:
  A textual description of the assertion

* `mapping` **(required)** *(type: string)*:
  The name of the mapping which is to be tested.
