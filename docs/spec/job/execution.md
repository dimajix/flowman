# Flowman Job Execution

When executing a specific job, Flowman first builds a dependency tree of all targets. Then the targets are executed
in an order such that all dependencies of each target have to be built before the target in question itself is being
built.

For building the dependency tree, Flowman examines the physical entities required or produced by each target. Therefore
in most cases, you do not need to manually specify dependencies yourself (although this is possible).

## Manual Dependencies


## Automatic Dependencies
