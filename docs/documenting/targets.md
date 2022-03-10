# Documenting Targets

Flowman also supports documenting build targets. There aren't many options or properties, since targets do not represent
any data or transformations themselves. Documenting them mainly serves to complete a technical reference for
developers.

## Example

```yaml
targets:
  stations:
    kind: relation
    description: "Write stations"
    mapping: stations_raw
    relation: stations
    
    documentation:
      description: "This build target is used to write the weather stations"
```
