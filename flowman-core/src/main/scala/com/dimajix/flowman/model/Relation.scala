/*
 * Copyright 2018-2019 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.model

import java.util.Locale

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.{OutputMode => StreamOutputMode}
import org.apache.spark.sql.types.StructType

import com.dimajix.common.Trilean
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.Dataset.Properties
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


object Relation {
    object Properties {
        def apply(context: Context, name:String = "") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                "",
                Map(),
                None,
                Map()
            )
        }
    }
    final case class Properties(
        context:Context,
        namespace:Option[Namespace],
        project:Option[Project],
        name:String,
        kind:String,
        labels:Map[String,String],
        description:Option[String],
        options:Map[String,String]
    )
    extends Instance.Properties[Properties] {
        override def withName(name: String): Properties = copy(name=name)
    }
}


/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
trait Relation extends Instance {
    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "relation"

    /**
      * Returns an identifier for this relation
      * @return
      */
    def identifier : RelationIdentifier

    /**
      * Returns a description of the relation
      * @return
      */
    def description : Option[String]

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    def provides : Set[ResourceIdentifier]

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    def requires : Set[ResourceIdentifier]

    /**
      * Returns the list of all resources which will are managed by this relation for reading or writing a specific
      * partition. The list will be specifically  created for a specific partition, or for the full relation (when the
      * partition is empty)
      * @param partitions
      * @return
      */
    def resources(partitions:Map[String,FieldValue] = Map()) : Set[ResourceIdentifier]

    /**
      * Returns the schema of the relation, excluding partition columns
      * @return
      */
    def schema : Option[Schema]

    /**
      * Returns the list of partition columns
      * @return
      */
    def partitions : Seq[PartitionField]

    /**
      * Returns a list of fields including the partition columns
      * @return
      */
    def fields : Seq[Field] = schema.toSeq.flatMap(_.fields) ++ partitions.map(_.field)

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    def read(executor:Executor, schema:Option[StructType], partitions:Map[String,FieldValue] = Map()) : DataFrame

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param executor
      * @param df - dataframe to write
      * @param partition - destination partition
      */
    def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue] = Map(), mode:OutputMode = OutputMode.OVERWRITE) : Unit

    /**
      * Removes one or more partitions.
      * @param executor
      * @param partitions
      */
    def truncate(executor:Executor, partitions:Map[String,FieldValue] = Map()) : Unit

    /**
      * Reads data from a streaming source
      * @param executor
      * @param schema
      * @return
      */
    def readStream(executor:Executor, schema:Option[StructType]) : DataFrame = ???

    /**
      * Writes data to a streaming sink
      * @param executor
      * @param df
      * @return
      */
    def writeStream(executor:Executor, df:DataFrame, mode:OutputMode, checkpointLocation:Path) : StreamingQuery = ???

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage. This refers to
      * the relation itself, not to the data or a specific partition. [[loaded]] should return [[Yes]] after
     *  [[[create]] has been called, and it should return [[No]] after [[destroy]] has been called.
 *
      * @param executor
      * @return
      */
    def exists(executor:Executor) : Trilean

    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     * @param executor
     * @param partition
     * @return
     */
    def loaded(executor:Executor, partition:Map[String,SingleValue] = Map()) : Trilean

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      * @param executor
      */
    def create(executor:Executor, ifNotExists:Boolean=false) : Unit

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      * @param executor
      */
    def destroy(executor:Executor, ifExists:Boolean=false) : Unit

    /**
      * This will update any existing relation to the specified metadata.
      * @param executor
      */
    def migrate(executor:Executor) : Unit
}



/**
 * Common base implementation for the Relation interface class. It contains a couple of common properties.
 */
abstract class BaseRelation extends AbstractInstance with Relation {
    protected override def instanceProperties : Relation.Properties

    /**
     * Returns an identifier for this relation
     * @return
     */
    override def identifier : RelationIdentifier = RelationIdentifier(name, project.map(_.name))

    /**
     * Returns a description for the relation
     * @return
     */
    override def description : Option[String] = instanceProperties.description

    /**
     * Returns the schema of the relation, excluding partition columns
     * @return
     */
    override def schema : Option[Schema] = None

    /**
     * Returns the list of partition columns
     * @return
     */
    override def partitions : Seq[PartitionField] = Seq()

    /**
     * Returns a map of all options. There is no specific usage for options, that depends on the
     * specific implementation
     * @return
     */
    def options : Map[String,String] = instanceProperties.options

    /**
     * Creates a DataFrameReader which is already configured with options and the schema is also
     * already included
     * @param executor
     * @return
     */
    protected def reader(executor:Executor) : DataFrameReader = {
        val reader = executor.spark.read.options(options)

        inputSchema.foreach(s => reader.schema(s))

        reader
    }

    /**
     * Creates a DataStreamReader which is already configured with options and the schema is also
     * already included
     * @param executor
     * @return
     */
    protected def streamReader(executor: Executor) : DataStreamReader = {
        val reader = executor.spark.readStream.options(options)

        inputSchema.foreach(s => reader.schema(s))

        reader
    }

    /**
     * Ceates a DataFrameWriter which is already configured with any options. Moreover
     * the desired schema of the relation is also applied to the DataFrame
     * @param executor
     * @param df
     * @return
     */
    protected def writer(executor: Executor, df:DataFrame) : DataFrameWriter[Row] = {
        val outputDf = applyOutputSchema(executor, df)
        outputDf.write.options(options)
    }

    /**
     * Ceates a DataStreamWriter which is already configured with any options. Moreover
     * the desired schema of the relation is also applied to the DataFrame
     * @param executor
     * @param df
     * @return
     */
    protected def streamWriter(executor: Executor, df:DataFrame, outputMode:StreamOutputMode, checkpointLocation:Path) : DataStreamWriter[Row]= {
        val outputDf = applyOutputSchema(executor, df)
        outputDf.writeStream
            .options(options)
            .option("checkpointLocation", checkpointLocation.toString)
            .outputMode(outputMode)
    }

    /**
     * Creates a Spark schema from the list of fields.
     * @return
     */
    protected def inputSchema : Option[StructType] = {
        schema.map(s => StructType(s.fields.map(_.sparkField)))
    }

    /**
     * Creates a Spark schema from the list of fields. The list is used for output operations, i.e. for writing
     * @return
     */
    protected def outputSchema : Option[StructType] = {
        schema.map(s => s.sparkSchema)
    }

    /**
     * Applies the specified schema (or maybe even transforms it)
     * @param df
     * @return
     */
    protected def applyOutputSchema(executor:Executor, df:DataFrame) : DataFrame = {
        SchemaUtils.applySchema(df, outputSchema)
    }
}



trait PartitionedRelation { this:Relation =>
    def partitions : Seq[PartitionField]

    /**
     * Applies a partition filter using the given partition values
     * @param df
     * @param partitions
     * @return
     */
    protected def filterPartition(df:DataFrame, partitions: Map[String, FieldValue]) : DataFrame = {
        val partitionSchema = PartitionSchema(this.partitions)

        def applyPartitionFilter(df: DataFrame, partitionName: String, partitionValue: FieldValue): DataFrame = {
            val field = partitionSchema.get(partitionName)
            val values = field.interpolate(partitionValue).toSeq
            df.filter(df(partitionName).isin(values: _*))
        }

        partitions.foldLeft(df)((df, pv) => applyPartitionFilter(df, pv._1, pv._2))
    }

    /**
     * Adds partition columns to an existing DataFrame
     *
     */
    protected def addPartition(df:DataFrame, partition:Map[String,SingleValue]) : DataFrame = {
        val partitionSchema = PartitionSchema(this.partitions)

        def addPartitioColumn(df: DataFrame, partitionName: String, partitionValue: SingleValue): DataFrame = {
            val field = partitionSchema.get(partitionName)
            val value = field.parse(partitionValue.value)
            df.withColumn(partitionName, lit(value))
        }

        partition.foldLeft(df)((df, pv) => addPartitioColumn(df, pv._1, pv._2))
    }

    protected def requireAllPartitionKeys(map: Map[String,_]) : Unit = {
        val partitionKeys = partitions.map(_.name.toLowerCase(Locale.ROOT)).toSet
        val valueKeys = map.keys.map(_.toLowerCase(Locale.ROOT)).toSet
        valueKeys.foreach(key => if (!partitionKeys.contains(key)) throw new IllegalArgumentException(s"Specified partition '$key' not defined in relation '$identifier'"))
        partitionKeys.foreach(key => if (!valueKeys.contains(key)) throw new IllegalArgumentException(s"Value for partition '$key' missing for relation '$identifier'"))
    }

    protected def requireValidPartitionKeys(map: Map[String,_]) : Unit = {
        val partitionKeys = partitions.map(_.name.toLowerCase(Locale.ROOT)).toSet
        val valueKeys = map.keys.map(_.toLowerCase(Locale.ROOT)).toSet
        valueKeys.foreach(key => if (!partitionKeys.contains(key)) throw new IllegalArgumentException(s"Specified partition '$key' not defined in relation '$identifier'"))
    }
}


/**
 * Common base implementation for the Relation interface class. It contains a couple of common properties.
 */
trait SchemaRelation { this: Relation =>
    /**
     * Returns the schema of the relation
     *
     * @return
     */
    override def schema : Option[Schema]
}
