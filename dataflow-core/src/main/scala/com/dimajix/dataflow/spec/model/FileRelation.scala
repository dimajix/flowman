package com.dimajix.dataflow.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.model.Relation.SingleValue
import com.dimajix.dataflow.spec.model.Relation.Value
import com.dimajix.dataflow.spec.schema.Field
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

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,Value] = Map()) : DataFrame = {
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
    private def collectFiles(executor: Executor, partitions:Map[String,Value]) = {
        implicit val context = executor.context
        val collector = new FileCollector(executor.spark)
            .location(location)
            .pattern(pattern)
        val inputFiles = collector.collect(partitions)

        // Print all files that we found
        inputFiles.foreach(f => logger.info("Reading input file {}", f.toString))
        inputFiles
    }
}
