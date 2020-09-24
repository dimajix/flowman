# Flowman Job Execution

When executing a specific job, Flowman first builds a dependency tree of all targets. Then the targets are executed
in an order such that all dependencies of each target have to be built before the target in question itself is being
built.

For building the dependency tree, Flowman examines the physical entities required or produced by each target. Therefore
in most cases, you do not need to manually specify dependencies yourself (although this is possible).

## Automatic Dependencies

Flowman tries to detect any dependencies between targets automatically, such that all targets are built in the correct
order. You need nothing to do to take advantage of this feature.


## Manual Dependencies

In addition to automatic dependency management, you can also specify explicit dependencies between targets. This can
be done by adding `before` and `after` tags to the targets. 

### Example
```yaml
targets:
  target_a:
    kind: relation
    before:
      - target_b
      - target_c
    ...

  target_b:
    kind:
    after: target_x
    ...

  ...
```
