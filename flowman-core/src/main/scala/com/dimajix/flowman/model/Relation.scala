/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.{OutputMode => StreamOutputMode}

import com.dimajix.common.MapIgnoreCase
import com.dimajix.common.SetIgnoreCase
import com.dimajix.common.Trilean
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.documentation.RelationDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MergeClause
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model
import com.dimajix.flowman.transforms.ColumnMismatchStrategy
import com.dimajix.flowman.transforms.SchemaEnforcer
import com.dimajix.flowman.transforms.TypeMismatchStrategy
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType


object Relation {
    object Properties {
        def apply(context: Context, name:String = "", kind:String = "") : Properties = {
            Properties(
                context,
                Metadata(context, name, Category.RELATION, kind),
                None,
                None
            )
        }
    }
    final case class Properties(
        context:Context,
        metadata:Metadata,
        description:Option[String],
        documentation:Option[RelationDoc]
    )
    extends model.Properties[Properties] {
        override val namespace : Option[Namespace] = context.namespace
        override val project : Option[Project] = context.project
        override val kind : String = metadata.kind
        override val name : String = metadata.name

        override def withName(name: String): Properties = copy(metadata=metadata.copy(name = name))

        def merge(other: Properties): Properties = {
            Properties(
                context,
                metadata.merge(other.metadata),
                other.description.orElse(description),
                documentation.map(_.merge(other.documentation)).orElse(other.documentation)
            )
        }
        def identifier : RelationIdentifier = RelationIdentifier(name, project.map(_.name))
    }
}


/**
  * Interface class for declaring relations (for sources and sinks) as part of a model
  */
trait Relation extends Instance {
    override type PropertiesType = Relation.Properties

    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: Category = Category.RELATION

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
     * Returns a (static) documentation of this relation
     * @return
     */
    def documentation : Option[RelationDoc]

    /**
      * Returns the list of all resources which will be created by this relation. This method mainly refers to the
      * CREATE and DESTROY execution phase.
      *
      * @return
      */
    def provides : Set[ResourceIdentifier]

    /**
      * Returns the list of all resources which will be required by this relation for creation. This method mainly
      * refers to the CREATE and DESTROY execution phase.
      *
      * @return
      */
    def requires : Set[ResourceIdentifier]

    /**
      * Returns the list of all resources which will are managed by this relation for reading or writing a specific
      * partition. The list will be specifically  created for a specific partition, or for the full relation (when the
      * partition is empty). This method mainly refers to the BUILD and TRUNCATE execution phase.
      * @param partitions
      * @return
      */
    def resources(partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier]

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
      * Returns a list of fields including the partition columns. This method should not perform any physical schema
      * inference.
      * @return
      */
    def fields : Seq[Field]

    /**
     * Returns the schema of the relation, either from an explicitly specified schema or by schema inference from
     * the physical source
     * @param execution
     * @param partitions - Optional partition as a hint for schema inference
     * @return
     */
    def describe(execution:Execution, partitions:Map[String,FieldValue] = Map.empty) : StructType

        /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param execution
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    def read(execution:Execution, partitions:Map[String,FieldValue] = Map.empty) : DataFrame

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param execution
      * @param df - dataframe to write
      * @param partition - destination partition
      */
    def write(execution:Execution, df:DataFrame, partition:Map[String,SingleValue] = Map.empty, mode:OutputMode = OutputMode.OVERWRITE) : Unit

    /**
     * Performs a merge operation. Either you need to specify a [[mergeKey]], or the relation needs to provide some
     * default key.
     * @param execution
     * @param df
     * @param mergeCondition
     * @param clauses
     */
    def merge(execution:Execution, df:DataFrame, condition:Option[Column], clauses:Seq[MergeClause]) : Unit = ???

    /**
      * Removes one or more partitions.
      * @param execution
      * @param partitions
      */
    def truncate(execution:Execution, partitions:Map[String,FieldValue] = Map.empty) : Unit

    /**
      * Reads data from a streaming source
      * @param execution
      * @param schema
      * @return
      */
    def readStream(execution:Execution) : DataFrame = ???

    /**
      * Writes data to a streaming sink
      * @param execution
      * @param df
      * @return
      */
    def writeStream(execution:Execution, df:DataFrame, mode:OutputMode, trigger:Trigger, checkpointLocation:Path) : StreamingQuery = ???

    /**
      * Returns true if the relation already exists, otherwise it needs to be created prior usage. This refers to
      * the relation itself, not to the data or a specific partition. [[loaded]] should return [[Yes]] after
     *  [[[create]] has been called, and it should return [[No]] after [[destroy]] has been called.
 *
      * @param execution
      * @return
      */
    def exists(execution:Execution) : Trilean

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     * @param execution
     * @return
     */
    def conforms(execution:Execution, migrationPolicy:MigrationPolicy=MigrationPolicy.RELAXED) : Trilean

    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     * @param execution
     * @param partition
     * @return
     */
    def loaded(execution:Execution, partition:Map[String,SingleValue] = Map.empty) : Trilean

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      * @param execution
      */
    def create(execution:Execution, ifNotExists:Boolean=false) : Unit

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      * @param execution
      */
    def destroy(execution:Execution, ifExists:Boolean=false) : Unit

    /**
      * This will update any existing relation to the specified metadata.
      * @param execution
      */
    def migrate(execution:Execution, migrationPolicy:MigrationPolicy=MigrationPolicy.RELAXED, migrationStrategy:MigrationStrategy=MigrationStrategy.ALTER) : Unit

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    def link(linker:Linker) : Unit
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
    override def identifier : RelationIdentifier = instanceProperties.identifier

    /**
     * Returns a description for the relation
     * @return
     */
    override def description : Option[String] = instanceProperties.description

    /**
     * Returns a (static) documentation of this relation
     * @return
     */
    override def documentation : Option[RelationDoc] = instanceProperties.documentation

    /**
     * Returns the schema of the relation, excluding partition columns
     * @return
     */
    override def schema : Option[Schema] = None

    /**
     * Returns the list of partition columns
     * @return
     */
    override def partitions : Seq[PartitionField] = Seq.empty

    /**
      * Returns a list of fields including the partition columns. This method should not perform any physical schema
      * inference.
      * @return
      */
    override def fields : Seq[Field] = {
        val partitions = this.partitions
        val partitionFields = SetIgnoreCase(partitions.map(_.name))
        schema.toSeq.flatMap(_.fields).filter(f => !partitionFields.contains(f.name)) ++ partitions.map(_.field)
    }

    /**
     * Returns the schema of the relation, either from an explicitly specified schema or by schema inference from
     * the physical source
     * @param execution
     * @return
     */
    override def describe(execution:Execution, partitions:Map[String,FieldValue] = Map.empty) : StructType = {
        val partitionNames = SetIgnoreCase(this.partitions.map(_.name))
        val result = if (!fields.forall(f => partitionNames.contains(f.name))) {
            // Use given fields if relation contains valid list of fields in addition to the partition columns
            StructType(fields)
        }
        else {
            // Otherwise let Spark infer the schema
            val df = read(execution, partitions)
            StructType.of(df.schema)
        }

        applyDocumentation(result)
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker:Linker) : Unit = {}

    /**
     * Creates a DataFrameReader which is already configured with the schema
     * @param execution
     * @return
     */
    protected def reader(execution:Execution, format:String, options:Map[String,String]) : DataFrameReader = {
        val reader = execution.spark.read
            .format(format)
            .options(options)

        inputSchema.foreach(s => reader.schema(s))

        reader
    }

    /**
     * Creates a DataStreamReader which is already configured
     * @param execution
     * @return
     */
    protected def streamReader(execution: Execution, format:String, options:Map[String,String]) : DataStreamReader = {
        val reader = execution.spark.readStream
            .format(format)
            .options(options)

        inputSchema.foreach(s => reader.schema(s))

        reader
    }

    /**
     * Ceates a DataFrameWriter which is already configured with any options. Moreover
     * the desired schema of the relation is also applied to the DataFrame
     * @param execution
     * @param df
     * @return
     */
    protected def writer(execution: Execution, df:DataFrame, format:String, options:Map[String,String], saveMode:SaveMode, dynamicPartitions:Boolean=false) : DataFrameWriter[Row] = {
        applyOutputSchema(execution, df, includePartitions = dynamicPartitions)
            .write
            .format(format)
            .options(options)
            .mode(saveMode)
    }

    /**
     * Ceates a DataStreamWriter which is already configured with any options. Moreover
     * the desired schema of the relation is also applied to the DataFrame
     * @param execution
     * @param df
     * @return
     */
    protected def streamWriter(execution: Execution, df:DataFrame, format:String, options:Map[String,String], outputMode:StreamOutputMode, trigger:Trigger, checkpointLocation:Path) : DataStreamWriter[Row]= {
        applyOutputSchema(execution, df, includePartitions=true)
            .writeStream
            .format(format)
            .options(options)
            .option("checkpointLocation", checkpointLocation.toString)
            .trigger(trigger)
            .outputMode(outputMode)
    }

    /**
     * Returns the schema that will be used internally when reading from this data source. This schema should match the
     * user specified schema and will be applied in read operations. This should include the partition column whenever
     * the source returns it.
     * @return
     */
    protected def inputSchema : Option[org.apache.spark.sql.types.StructType] = {
        schema.map(_.sparkSchema)
    }

    /**
     * Returns the Spark schema as it is expected from the physical relation for write operations. The list is used
     * for output operations, i.e. for writing. This should include partitions only if they are required for write
     * operations.
     * @return
     */
    protected def outputSchema(execution:Execution) : Option[org.apache.spark.sql.types.StructType] = {
        schema.map(_.catalogSchema)
    }

    /**
     * Returns the full schema including partition columns
     * @return
     */
    protected def fullSchema : Option[StructType] = {
        schema.map { schema =>
            val schemaFieldNames = SetIgnoreCase(schema.fields.map(_.name))
            val partitionFieldNames = SetIgnoreCase(partitions.map(_.name))
            val schemaFields = schema.fields.map {
                case f:Field if partitionFieldNames.contains(f.name) => f.copy(nullable = false)
                case f => f
            }
            StructType(schemaFields ++ partitions.filter(p => !schemaFieldNames.contains(p.name)).map(_.field))
        }
    }

    protected def appendPartitionColumns(df:DataFrame) : DataFrame = {
        val schemaFields = MapIgnoreCase(df.schema.fields.map(p => p.name -> p))

        partitions.foldLeft(df) { case(df,p) =>
            if (schemaFields.contains(p.name))
                df
            else
                df.withColumn(p.name, lit(null).cast(p.sparkType))
        }
    }

    protected def appendPartitionColumns(schema:org.apache.spark.sql.types.StructType) : org.apache.spark.sql.types.StructType = {
        val schemaFieldNames = SetIgnoreCase(schema.fieldNames)
        val partitionFieldNames = SetIgnoreCase(partitions.map(_.name))
        val schemaFields = schema.fields.map {
            case f:org.apache.spark.sql.types.StructField if partitionFieldNames.contains(f.name) => f.copy(nullable = false)
            case f => f
        }
        org.apache.spark.sql.types.StructType(schemaFields ++ partitions.filter(p => !schemaFieldNames.contains(p.name)).map(_.catalogField))
    }

    /**
     * Applies the output schema (or maybe even transforms it). This should include partitions only if they are
     * required for write operations.
     * @param df
     * @return
     */
    protected def applyOutputSchema(execution:Execution, df:DataFrame, includePartitions:Boolean=false) : DataFrame = {
        // Add all partition columns to the output schema
        val outputSchema = this.outputSchema(execution)
            .map { schema =>
                if (includePartitions)
                    appendPartitionColumns(schema)
                else
                    schema
            }

        // Use SchemaEnforcer to enforce current outputSchema
        outputSchema.map { schema =>
                val conf = execution.flowmanConf
                val enforcer = SchemaEnforcer(
                    schema,
                    columnMismatchStrategy = ColumnMismatchStrategy.ofString(conf.getConf(FlowmanConf.DEFAULT_RELATION_OUTPUT_COLUMN_MISMATCH_STRATEGY)),
                    typeMismatchStrategy = TypeMismatchStrategy.ofString(conf.getConf(FlowmanConf.DEFAULT_RELATION_OUTPUT_TYPE_MISMATCH_STRATEGY))
                )
                enforcer.transform(df)
            }
            .getOrElse(df)
    }

    protected def applyInputSchema(execution:Execution, df:DataFrame, includePartitions:Boolean=true) : DataFrame = {
        // Add all partition columns to the output schema
        val inputSchema = this.inputSchema
            .map { schema =>
                if (includePartitions)
                    appendPartitionColumns(schema)
                else
                    schema
            }

        // Use SchemaEnforcer to enforce current inputSchema
        inputSchema.map { schema =>
                val conf = execution.flowmanConf
                val enforcer = SchemaEnforcer(
                    schema,
                    columnMismatchStrategy = ColumnMismatchStrategy.ofString(conf.getConf(FlowmanConf.DEFAULT_RELATION_INPUT_COLUMN_MISMATCH_STRATEGY)),
                    typeMismatchStrategy = TypeMismatchStrategy.ofString(conf.getConf(FlowmanConf.DEFAULT_RELATION_INPUT_TYPE_MISMATCH_STRATEGY))
                )
                enforcer.transform(df)
            }
            .getOrElse(df)
    }

    protected def applyDocumentation(schema:StructType) : StructType = {
        documentation
            .flatMap(_.schema.map(_.enrich(schema)))
            .getOrElse(schema)
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

        def addPartitionColumn(df: DataFrame, partitionName: String, partitionValue: SingleValue): DataFrame = {
            val field = partitionSchema.get(partitionName)
            val value = field.parse(partitionValue.value)
            df.withColumn(partitionName, lit(value))
        }

        partition.foldLeft(df)((df, pv) => addPartitionColumn(df, pv._1, pv._2))
    }

    /**
     * This method ensures that the given map contains entries for all partitions of the relations. It also ensures
     * that the map does not contain any additional key which is not a partition key
     * @param map
     */
    protected def requireAllPartitionKeys(map: Map[String,_]) : Unit = {
        val partitionKeys = SetIgnoreCase(partitions.map(_.name))
        val valueKeys = SetIgnoreCase(map.keys)
        valueKeys.foreach(key => if (!partitionKeys.contains(key)) throw new IllegalArgumentException(s"Specified partition '$key' not defined in relation '$identifier'"))
        partitionKeys.foreach(key => if (!valueKeys.contains(key)) throw new IllegalArgumentException(s"Value for partition '$key' missing for relation '$identifier'"))
    }

    /**
     * This method ensures that the given map contains entries for all partitions of the relations. It also ensures
     * that the map does not contain any additional key which is not a partition key. In this method the partition
     * keys can also be contained in the second argument [[columns]], which would be the columns of a DataFrame
     * when dynamically writing to partitions.
     * @param map
     */
    protected def requireAllPartitionKeys(parts: Map[String,_], columns:Iterable[String]) : Unit = {
        val partitionKeys = SetIgnoreCase(partitions.map(_.name))
        val valueKeys = SetIgnoreCase(parts.keys)
        val columnKeys = SetIgnoreCase(columns)
        valueKeys.foreach(key => if (!partitionKeys.contains(key)) throw new IllegalArgumentException(s"Specified partition '$key' not defined in relation '$identifier'"))
        partitionKeys.foreach(key => if (!valueKeys.contains(key) && !columnKeys.contains(key)) throw new IllegalArgumentException(s"Value for partition '$key' missing for relation '$identifier'"))
    }

    /**
     * This method ensures that the given map does not contain any additional key which is not a partition key
     * @param map
     */
    protected def requireValidPartitionKeys(map: Map[String,_]) : Unit = {
        val partitionKeys = SetIgnoreCase(partitions.map(_.name))
        val valueKeys = SetIgnoreCase(map.keys)
        valueKeys.foreach(key => if (!partitionKeys.contains(key)) throw new IllegalArgumentException(s"Specified partition '$key' not defined in relation '$identifier'"))
    }
}


/**
 * Common base implementation for the Relation interface class. It contains a couple of common properties.
 */
trait SchemaRelation { this: Relation =>
    /**
     * Returns the list of all resources which will be required by this relation for creation. This method mainly
     * refers to the CREATE and DESTROY execution phase.
     *
     * @return
     */
    override def requires: Set[ResourceIdentifier] = schema.map(_.requires).getOrElse(Set())

    /**
     * Returns the schema of the relation
     *
     * @return
     */
    override def schema : Option[Schema]
}
