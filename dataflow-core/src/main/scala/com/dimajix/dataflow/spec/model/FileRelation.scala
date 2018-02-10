package com.dimajix.dataflow.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.schema.Field
import com.dimajix.dataflow.spec.schema.SingleValue
import com.dimajix.dataflow.spec.schema.FieldValue
import com.dimajix.dataflow.util.FileCollector


class FileRelation extends BaseRelation {
    private val logger = LoggerFactory.getLogger(classOf[FileRelation])

    @JsonProperty(value="location") private var _location: String = _
    @JsonProperty(value="format") private var _format: String = "csv"
    @JsonProperty(value="partitions") private var _partitions: Seq[Field] = Seq()
    @JsonProperty(value="pattern") private var _pattern: String = _

    def pattern(implicit context:Context) : String = context.evaluate(_pattern)
    def location(implicit context:Context) : String = context.evaluate(_location)
    def format(implicit context:Context) : String = context.evaluate(_format)
    def partitions : Seq[Field] = _partitions

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        implicit val context = executor.context
        val inputFiles = collectFiles(executor, partitions)

        val reader = this.reader(executor).format(format)
        val rawData = reader.load(inputFiles.map(_.toString):_*)

        //applySchema(rawData, requestedSchema)
        rawData
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param executor
      * @param df - dataframe to write
      * @param partition - destination partition
      */
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = ???

    override def create(executor:Executor) : Unit = ???
    override def destroy(executor:Executor) : Unit = ???
    override def migrate(executor:Executor) : Unit = ???

    /**
      * Collects files for a given time period using the pattern inside the specification
      *
      * @param executor
      * @param partitions
      * @return
      */
    private def collectFiles(executor: Executor, partitions:Map[String,FieldValue]) = {
        implicit val context = executor.context
        val partitionColumnsByName = this.partitions.map(kv => (kv.name,kv)).toMap
        val resolvedPartitions = partitions.map(kv => (kv._1, partitionColumnsByName(kv._1).interpolate(kv._2).map(_.toString)))
        val collector = new FileCollector(executor.spark)
            .path(new Path(location))
            .pattern(pattern)
        val inputFiles = collector.collect(resolvedPartitions)

        // Print all files that we found
        inputFiles.foreach(f => logger.info("Reading input file {}", f.toString))
        inputFiles
    }
}
