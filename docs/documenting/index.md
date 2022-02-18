# Documenting with Flowman

Flowman supports to automatically generate a documentation of your project. The documentation can either include all
major entities like mappings, relations and targets. Or you may want to focus only on some aspects like the relations
which is useful for providing a documentation of the data model.

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```

### Providing Descriptions

Although Flowman will generate many valuable documentation bits by inspecting the project, the most important entities
(relations, mappings and targets) also provide the ability to manually and explicitly add documentation to them. This
documentation will override any automatically inferred information.


### Generating Documentation via Command Line

Generating the documentation is as easy as running [flowexec](../cli/flowexec.md) as follows:

```shell
flowexec -f my_project_directory documentation generate
```

Since generating documentation also requires a job context (which may contain additional parameters and environment
variables), you can also explicitly specify the job which is used for instantiating all entities like relations,
mappings and targets as follows:

```shell
flowexec -f my_project_directory documentation generate <job_name> <param_1=value> <param_2=value>
```
If no job is specified, Flowman will use the `main` job


### Generating Documentation via Build Target

The section above descirbes how to explicitly generate the project documentation by invoking 
`flowexec documentation generate`. As an alternative, Flowman offers a [document](../spec/target/document.md)
targets, which allows one to generate the documentation during the `VERIFY` phase (after the `BUILD` phase has
finished) of a normal Flowman project.

This can be easily configured as follows

```yaml
targets:
  # This target will create a documentation in the VERIFY phase
  doc:
    kind: documentation
    # We do not specify any additional configuration, so the project's documentation.yml file will be used
```

Then you only need to add that build target `doc` to your job as follows:

```yaml
jobs:
  main:
    targets:
      # List all targets which should be built as part of the `main` job
      - measurements
      - ...
      # Finally add the "doc" job for generating the documentation 
      - doc
```
