# Null Target

The `null` target is a dummy target, mainly used for testing purposes. In contrast to the 
[Blackhole Target](blackhole.md), the `null` target does not provide an input mapping and supports all build phases, 
but the target is never *dirty*. This means that the target will only be executed when the `--force` option is specified.

## Example
```yaml
targets:
  dummy:
    kind: null
```


## Fields

* `kind` **(mandatory)** *(type: string)*: `null`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target


## Supported Execution Phases
* `CREATE`
* `MIGRATE`
* `BUILD`
* `VERIFY`
* `TRUNCATE`
* `DESTROY`

Read more about [execution phases](../../lifecycle.md).
