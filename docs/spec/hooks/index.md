# Execution Hooks

Flowman provides the ability to specify so called *hooks*, which are called during lifecycle execution for every job
and target. For example by using the `web` hook, you can inform an external system about successful processing of
jobs and targets.

Hooks can be specified both on a global [namespace](../namespace.md) level and on a [job](../job/index.md) level.


## Hook Types

Flowman supports different kinds of hooks, the following list gives you an exhaustive overview of all hooks implemented
by Flowman

```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```
  
