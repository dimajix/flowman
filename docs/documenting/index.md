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

## Providing Descriptions

Although Flowman will generate many valuable documentation bits by inspecting the project, the most important entities
(relations, mappings and targets) also provide the ability to manually and explicitly add documentation to them. This
documentation will override any automatically inferred information.


## Generating Documentation

Generating the documentation is as easy as running [flowexec](../cli/flowexec.md) as follows:

```shell
flowexec -f my_project_directory documentation generate
```
