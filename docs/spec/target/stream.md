# Stream Target

The `stream` target is used for starting continuous streaming jobs, which for example read from and write to Kafka.
Only some relation types actually support streaming


## Example

```yaml
targets:
  my_stream:
    kind: stream
    relation: kafka_sink
    mapping: source_data
    mode: append
    trigger: 5 seconds

relations:
  kafka_sink:
    kind: kafka
    topics: generic_events
```

Since Flowman 0.18.0, you can also directly specify the relation inside the target definition. This saves you
from having to create a separate relation definition in the `relations` section. This is only recommeneded, if you
do not access the target relation otherwise, such that a shared definition would not provide any benefir.
```yaml
targets:
  my_stream:
    kind: stream
    relation:
      kind: kafka
      topics: generic_events
    mapping: source_data
    mode: append
    trigger: 5 seconds
```


## Fields

* `kind` **(mandatory)** *(type: string)*: `stream`

* `description` **(optional)** *(type: string)*:
  Optional descriptive text of the build target

* `mapping` **(optional)** *(type: string)*:
  Specifies the name of the input mapping to be read from

* `relation` **(mandatory)** *(type: string or relation)*:
  Specifies the name of the relation to write to. You can also specify an inline relation, as shown in the example

* `mode` **(optional)** *(type: string)* *(default=update)*:
  Specifies how the results are written to the streaming sink. Options include:
    * `complete`: complete output mode
    * `append`: append the data.
    * `update`: update output mode

* `checkpointLocation` **(optional)** *(type: string)* *(default=empty)*:
  Specifies the checkpoint location where Spark periodically stores the current state of the streaming query such
  that the application correctly continues from the last state when restartet. The checkpoint location needs to be
  accessible from all executors, i.e. it needs to reside on some shared storage (HDFS, S3, ...). 

* `trigger` **(optional)** *(type: string)* *(default=empty)*:
  Specifies the trigger interval at which executes each micro batch of the streaming query. If left empty, an interval
  of 0 milliseconds is used which equates to "as fast as possible". The special value `once` will process all the 
  currently available data and then stops processing. This kind of turns the stream processing into a batch processing
  again.

* `parallelism` **(optional)** *(type: integer)* *(default=16)*:
  This specifies the parallelism to be used when writing data. The parallelism equals the number
  of files being generated in HDFS output and also equals the maximum number of threads that are used in total in all
  Spark executors to produce the output. If `parallelism` is set to zero or to a negative number, Flowman will not
  coalesce any partitions and generate as many files as Spark partitions. The default value is controlled by the
  Flowman config variable `floman.default.target.parallelism`.

* `rebalance` **(optional)** *(type: bool)* *(default=false)*:
  Enables rebalancing the size of all partitions by introducing an additional internal shuffle operation. Each partition
  and output file will contain approximately the same number of records. The default value is controlled by the
  Flowman config variable `floman.default.target.rebalance`.
  
