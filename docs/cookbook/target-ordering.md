# Manual Target Execution Order

When executing a [job](../spec/job/index.md), Flowman normally figures out the correct execution order of all
[targets](../spec/target/index.md) automatically. This is implemented by looking at the different targets inputs
and outputs, such that Flowman ensures that first all the inputs of a target is build before the target itself is
executed.

## Cyclic Dependencies
But sometimes, this does not give you the desired result, or Flowman might even detect a cyclic dependency between 
your targets. Although this might indicate an issue at the conceptional level, there are completely valid use cases
where apparently cyclic dependencies are not an actual problem. But Flowman would still refuse to execute a job with
cyclic dependencies in its default configuration.


## Manual Scheduler
To overcome issues with a wrong (or unsolvable) execution order, Flowman provides a mechanism to turn off the automatic
execution ordering. This is achieved by setting the following conifguration variable

```yaml
config:
  - flowman.execution.scheduler.class=com.dimajix.flowman.execution.ManualScheduler
```

This setting will replace the usual dependency scheduler by a manual scheduler, where all targets are executed precisely
in the order as specified within a job. This also implies that when using the `ManualScheduler`, the developer is both
in complete control of execution order but also fully responsible for a correct execution order.
