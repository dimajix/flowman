# Flowman Schema Generator
The Flowman schema generator is a small utility application which generates a YAML schema file from the current
configuration including all plugins. This YAML schema file then can be used to in appropriate code editors which
support schema validation of YAML files. This will support developers in creating Flowman projects by enabling better
syntax highlighting and code auto-completion.

```shell
flowman-schema -o my-schema-directory
```

This command will create three different YAML schema files:
* `module.json` - This is the YAML schema for all modules, i.e. defining relations, mapping, etc.
* `project.sjon` - This YAML schema file contains all entities of the [`project.yml`](../spec/project.md) file.
* `namespace.sjon` - This YAML schema file contains all entities of [namespace definitions](../spec/namespace.md).


## Provided Schemas

Currently, there is no readily provided schema for Flowman available. This means that you really have to run the
command above to generate a YAML schema file for your environment. The reasoning behind not providing schema files is
simple: Since the available entities and YAML tags depend on the presence of plugins, the best solution is provide
some means to generate a YAML schema file precisely for your configuration. 


## Supported Editors

JSON schema definition files are supported by many code editors. The following editors are known to work well with
validation and auto-completion with the Flowman YAML schema files:

* JetBrains IntelliJ (and probably other IDEs from the JetBrains family)
* Microsoft Visual Studio Code (via the [YAML plugin](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml))

Please refer to the corresponding documentation how to configure each editor to use the YAML schema definitions.
