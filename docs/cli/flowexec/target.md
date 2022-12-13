# Target Commands
It is also possible to perform actions on individual targets using the `target` command group. In most cases this is
inferior to using one of the `job` commands, since typical jobs will also define appropriate environment variables
which might be required by targets.


## `list` - List all Targets
In order to retrieve all defined execution targets, run the following command
```shell
flowexec target list
```

## `validate|create|build|verify|truncate|destroy` - Execute Target phase
This set of commands is used for *executing a target phase*, or a complete lifecycle containing multiple individual
phases.
```shell
flowexec target <validate|create|build|verify|truncate|destroy> <target_name>
```
This will execute an individual target by executing the desired lifecycle for the `main` job. Additional parameters are
* `-h` displays help
* `-f` or `--force` force execution of the project, even if the output targets already exist.
* `-nl` or `--no-lifecycle` only execute the specified lifecycle phase, without all preceeding phases. For example
  the whole lifecycle for `verify` includes the phases `create` and `build` and these phases would be executed before
  `verify`. If this is not what you want, then use the option `-nl`


## `inspect` - Retrieving General Information
The `target inspect` commands provides some general information on an individual target, for example the active
execution phases and the resource dependencies.

```shell
flowexec -f ../examples/weather/ target inspect metrics
```
