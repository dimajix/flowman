# Document Target

The `document` (or equivalently `documentation`) target is used to build a documentation of the current project.
You can find more details about that feature in the [documentation section](../../documenting/index.md). You can either
generate the project documentation via `flowexec documentation generate`, or you also generate the documentation via
this special target, which will be executed as part of the `VERIFY` phsae (after the `BUILD` phase has finished).

## Example

```yaml
targets:
  documentation:
    kind: documentation
    collectors:
      # Collect documentation of relations
      - kind: relations
      # Collect documentation of mappings
      - kind: mappings
      # Collect documentation of build targets
      - kind: targets
      # Execute all tests
      - kind: tests
    
    generators:
      # Create an output file in the project directory
      - kind: file
        location: ${project.basedir}/generated-documentation
        template: html
        excludeRelations:
          # You can either specify a name (without the project)
          - "stations_raw"
          # Or can also explicitly specify a name with the project
          - ".*/measurements_raw"
```

## Fields

* `kind` **(mandatory)** *(type: string)*: `documentation` or `document`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `collectors` **(optional)** *(type: list:collector)*:
 List of documentation collectors

* `generators` **(optional)** *(type: list:generator)*:
  List of documentation generators


## Configuration

When no explicit configuration is provided via `generators` or `collectors`, then Flowman will use the
[documentation configuration](../../documenting/config.md) provided in `documentation.yml`. If that file does not
exist, Flowman will fall back to some default configuration, which creates a html based documentation in a
subdirectory `generated-documentation` within the projects base directory.


## Supported Execution Phases
* `VERIFY` - This will generate the documentation

Read more about [execution phases](../../concepts/lifecycle.md).


## Dirty Condition
A `document` target is always dirty, thereby overwriting any existing documentation with the newest information.
