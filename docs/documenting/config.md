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
  # Execute all tests
  - kind: tests

generators:
  # Create an output file in the project directory
  - kind: file
    location: ${project.basedir}/doc
    # This will exclude all mappings
    excludeMappings: ".*"
    excludeRelations:
      # You can either specify a name (without the project)
      - "stations_raw"
      # Or can also explicitly specify a name with the project
      - ".*/measurements_raw"
```

## Collectors

Flowman uses so called *collectors* which create an internal model of the documentation from the core entities like
relations, mappings and build targets. The default configuration uses the three collectors `relations`, `mappings` 
and `targets`, with each of them being responsible for one entity type. If you really do not require documentation
for one of these targets, you may want to simply remove the corresponding collector from that list.


## File Generator Fields

The generator is used for generating the documentation. You can configure multiple generators for creating multiple
differently configured documentations.

* `kind` **(mandatory)** *(type: string)*: `file` 

* `location` **(mandatory)** *(type: string)*: Specifies the output location 

* `includeMappings` **(optional)** *(type: list:regex)* *(default: ".*")*:
List of regular expressions which mappings to include. Per default all mappings will be included in the output.
The list of filters will be applied before the `excludeMappings` filter list.

* `excludeMappings` **(optional)** *(type: list:regex)*
  List of regular expressions which mappings to exclude. Per default no mapping will be excluded in the output.
  The list of filters will be applied after the `includeMappings` filter list.

* `includeTargets` **(optional)** *(type: list:regex)* *(default: ".*")*:
  List of regular expressions which targets to include. Per default all targets will be included in the output.
  The list of filters will be applied before the `excludeTargets` filter list.

* `excludeTargets` **(optional)** *(type: list:regex)*
  List of regular expressions which targets to exclude. Per default no target will be excluded in the output.
  The list of filters will be applied after the `includeTargets` filter list.

* `includeRelations` **(optional)** *(type: list:regex)* *(default: ".*")*:
  List of regular expressions which relations to include. Per default all relations will be included in the output.
  The list of filters will be applied before the `excludeRelations` filter list.

* `excludeRelations` **(optional)** *(type: list:regex)*
  List of regular expressions which relations to exclude. Per default no relation will be excluded in the output.
  The list of filters will be applied after the `includeRelations` filter list.
