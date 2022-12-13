# Flowman Job Execution

Flowman provides some robust execution mechanics, which ensure correct build results. These mechanics revolve
around execution phases and data dependencies.


## Execution Phases

Flowman sees data as artifacts with a common lifecycle, from creation until deletion. The lifecycle itself consists of
multiple different phases, each of them representing one stage of the whole lifecycle.

The full lifecycle consists out of specific execution phases, as follows:

1. **VALIDATE**.
   This first phase is used for validation and any error will stop the next steps. A validation step might for example
   check preconditions on data sources which are a hard requirement.

2. **CREATE**.
   This will create all relations (tables and directories) specified as targets. The tables will not contain any data,
   they only provide an empty hull. If a target already exists, a migration will be started instead. This will migrate a
   relation (table or directory) to a new schema, if required. Note that this is not supported by all target types, and
   even if a target supports migration in general, it may not be possible due to unmigratable changes.

3. **BUILD**.
   The *build* phase will actually create all records and fill them into the specified relations.

4. **VERIFY**.
   The *verify* phase will perform simple checks (for example if a specific Hive partition exists), or may also include
   some specific user defined tests that compare data. If verification fails, the build process stops.

5. **TRUNCATE**.
   *Truncate* is the first of two phases responsible for cleanup. *Truncate* will only remove individual partitions from
   tables (i.e. it will delete data), but it will keep tables alive.

6. **DESTROY**.
   The final phase *destroy* is used to physically remove relations including their data. This will also remove table
   definitions, views and directories. It performs the opposite operation than the *create* phase.


## Dependencies
When executing a specific job, Flowman first builds a dependency tree of all targets. Then the targets are executed
in an order such that all dependencies of each target have to be built before the target in question itself is being
built.

For building the dependency tree, Flowman examines the physical entities required or produced by each target. Therefore
in most cases, you do not need to manually specify dependencies yourself (although this is possible).

### Automatic Dependencies

Flowman tries to detect any dependencies between targets automatically, such that all targets are built in the correct
order. You need nothing to do to take advantage of this feature.


### Manual Dependencies

In addition to automatic dependency management, you can also specify explicit dependencies between targets. This can
be done by adding `before` and `after` tags to the targets. 

#### Example
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


## Full Execution Model

A [job](../job/index.md) groups multiple [targets](../target/index.md) to a logical bundle, which should be
built together. When executing a lifecycle for a job, Flowman will apply the following logic:

1. Iterate over all execution phases of the lifecycle (i.e. VALIDATE, CREATE, BUILD, VERIFY)
2. Perform dependency analysis of all targets within the job, which are active for the current execution phase
3. Execute all targets within the current phase
