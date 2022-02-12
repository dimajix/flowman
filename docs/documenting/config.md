# Configuring the Documentation

Flowman has a sound default for generating documentation for relations, mappings and targets. But you might want
to explicitly influence the way for what and how documentation is generated. This can be easily done by supplying
a `documentation.yml` file at the root level of your project (so it would be a sibling of the `project.yml` file).


## Example

```yaml
collectors:
  # Collect documentation of relations
  - kind: relations
  # Collect documentation of mappings
  - kind: mappings
  # Collect documentation of build targets
  - kind: targets

generators:
  # Create an output file in the project directory
  - kind: file
    location: ${project.basedir}/doc
```
