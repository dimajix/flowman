# Build Targets

From a top level perspective, Flowman works like a build tool like make or maven. Of course in contrast to classical
build tools, the project specification in Flowman also contains the logic to be build (normally that is separated
in source code files which get compiles or otherwise processed with additional tools).

Each target supports at least some [build phases](../../lifecycle.md)


## Target Types
Flowman supports different target types, each used for a different kind of a physical entity or build recipe.

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```

## Metrics

For each target Flowman provides the following execution metric:
* `metric`: "target_runtime"
* labels: 
  * `category`: "target"
  * `kind`: The kind of the target
  * `namespace`: The name of the namespace
  * `project`: The name of the project 
