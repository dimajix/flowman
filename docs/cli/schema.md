# Flowman Schema Generator
The Flowman schema generator is a small utility application which generates a YAML schema file from the current
configuration including all plugins. This YAML schema file then can be used in appropriate code editors which
support schema validation of YAML files. This will support developers in creating Flowman projects.

```shell
flowman-schema -o my-schema-directory
```

## Provided Schemas

Currently, there is no readily provided schema for Flowman available. This means that you really have to run the
command above to generate a YAML schema file for your environment. The reasoning behind not providing schema files is
simple: Since the available entities and YAML tags depend on the presence of plugins, the best solution is provide
some means to generate a YAML schema file precisely for your configuration. 


## Supported Editors

JSON schema definition files are supported by many code editors. The following editors are known to work well with
validation and auto-completion with the Flowman YAML schema files:

* JetBrains IntelliJ (and probably other editors from the JetBrains family)
* Microsoft Visual Studio Code (via the YAML plugin)

Please refer to the corresponding documentation how to configure each editor to use the YAML schema definitions.
