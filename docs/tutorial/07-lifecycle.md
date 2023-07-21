# Lesson 7 — Execution Phases and Lifecycles

Flowman implements a lifecycle model for managing physical data containers like files and tables. This lifecycle
model simplifies typical engineering tasks, like creating the required output tables, populating them with data
or removing individual data partitions.

## 1. What to Expect

### Objectives

* You will learn about the basic idea of the lifecycle model
* You will learn how to execute specific execution phases in your project


### Description

Since this chapter only describes an additional development tool for Flowman, we will simply reuse the project of the
last chapter. The project includes one job with multiple targets and relations.


## 2. Execution Phases
Flowman sees data as artifacts with a common lifecycle, from creation until deletion. The lifecycle itself consists of
multiple different phases, each of them representing one stage of the whole lifecycle.

### 2.1 Lifecycle Phases
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


### 2.2 Built In Lifecycles

Some execution phases can be performed in a meaningful way one after the other. Such a sequence of phases is
called *lifecycle*. Flowman has the following lifecycles built in:

#### Build
The first lifecycle contains the three phases *VALIDATE*, *CREATE*, *BUILD* and *VERIFY*.

#### Truncate
The second lifecycle contains only the single phase *TRUNCATE*

#### Destroy
The last lifecycle contains only the single phase *DESTROY*


### 2.3 Targets & Lifecycles

Each [target](../spec/target/index.md) supports a certain subset of execution phases. Not all targets support all
phases. For example, the widely used [`relation` target](../spec/target/relation.md) which is used for creating data
sinks and for writing new data into them supports the phases `CREATE`, `BUILD`, `VERIFY`, `TRUNCATE` and `DESTROY`. On
the other hand, the [`measure` target](../spec/target/measure.md) which collects some data dependent metrics is only
executed during the `VERIFY` phase.

Of course, when a specific target participates in multiple execution phases, it will perform different actions in each
of the phases. The documentation of each target will contain the details of the supported phases and what action is
performed in each of them.


### 2.4 Jobs & Lifecycles

A [job](../spec/job/index.md) groups multiple [targets](../spec/target/index.md) to a logical bundle, which should be
built together. When executing a lifecycle for a job, Flowman will apply the following logic:

1. Interpolate any given parameter given on the command line, for example
   `flowexec job daily verify processing_date:start=2022-11-01 processing_date:end=2022-11-10`
   would cycle through 10 consecutive days and execute the job `daily` with the parameter `processing_date` set
   accordingly.
2. Iterate over all execution phases of the lifecycle (i.e. VALIDATE, CREATE, BUILD, VERIFY).
3. Perform dependency analysis of all targets within the job, which are active for the current execution phase
4. Identify all active targets in the current phase (possibly accordingly to the jobs `executions` section)
5. Check each target if it is *dirty* (i.e. it requires an execution) the current phase
6. Execute all active and dirty targets in the correct order


## 3. Execution of Phases and Lifecycles

Another lifecycle can be executed by replacing `build` with another execution phase. Flowman will then execute the
lifecycle containing that phase up until this phase. If you want to skip all other execution phases, you can specify
`--no-lifecycle` on the command line.

```shell
flowexec -f <project_dirctory> job <execution_phase> <job_name> [--force] [--no-lifecycle]
```

The following examples will reuse the project of chapter 5.

### Perform full `BUILD` lifecycle

```shell
flowexec -f lessons/05-multiple-targets job build main
```

### Perform full `DESTROY` lifecycle

```shell
flowexec -f lessons/05-multiple-targets job destroy main
```

### Perform only `CREATE` phase

```shell
flowexec -f lessons/05-multiple-targets job create main --no-lifecycle
```

### Perform only `BUILD` phase

```shell
flowexec -f lessons/05-multiple-targets job build main --no-lifecycle
```


## 4. Next Lesson
In the next lesson, we will learn how to integrate relational databases as data sources and sinks by connecting them 
to Flowman via JDBC.
