/*
 * Copyright 2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.relation

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.util.SchemaUtils


case class GenericRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema],
    format:String
) extends BaseRelation with SchemaRelation {
    private val logger = LoggerFactory.getLogger(classOf[FileRelation])

    /**
     * Returns the list of all resources which will be created by this relation.
     *
     * @return
     */
    override def provides : Set[ResourceIdentifier] = Set()

    /**
     * Returns the list of all resources which will be required by this relation
     *
     * @return
     */
    override def requires : Set[ResourceIdentifier] = Set()

    /**
     * Returns the list of all resources which will be required by this relation for reading a specific partition.
     * The list will be specifically  created for a specific partition, or for the full relation (when the partition
     * is empty)
     *
     * @param partitions
     * @return
     */
    override def resources(partitions: Map[String, FieldValue]): Set[ResourceIdentifier] = Set()

    /**
     * Reads data from the relation, possibly from specific partitions
     *
     * @param executor
     * @param schema - the schema to read. If none is specified, all available columns will be read
     * @param partitions - List of partitions. If none are specified, all the data will be read
     * @return
     */
    override def read(executor:Executor, schema:Option[StructType], partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(executor != null)
        require(schema != null)
        require(partitions != null)

        val data = reader(executor).load()
        SchemaUtils.applySchema(data, schema)
    }

    /**
     * Writes data into the relation, possibly into a specific partition
     * @param executor
     * @param df - dataframe to write
     * @param partition - destination partition
     */
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode = OutputMode.OVERWRITE) : Unit = {
        require(executor != null)
        require(df != null)
        require(partition != null)

        writer(executor, df)
            .mode(mode.batchMode)
            .save()
    }

    /**
     * Returns true if the relation already exists, otherwise it needs to be created prior usage
     * @param executor
     * @return
     */
    override def exists(executor:Executor) : Boolean = true

    /**
     * This method will create the given directory as specified in "location"
     * @param executor
     */
    override def create(executor:Executor, ifNotExists:Boolean=false) : Unit = {}

    /**
     * This will update any existing relation to the specified metadata. Actually for this file based target, the
     * command will precisely do nothing.
     *
     * @param executor
     */
    override def migrate(executor:Executor) : Unit = {}

    /**
     * Removes one or more partitions.
     * @param executor
     * @param partitions
     */
    override def truncate(executor:Executor, partitions:Map[String,FieldValue] = Map()) : Unit = {}

    /**
     * This method will remove the given directory as specified in "location"
     * @param executor
     */
    override def destroy(executor:Executor, ifExists:Boolean) : Unit =  {}

    /**
     * Creates a DataFrameReader with bells and whistles configured from the specification
     *
     * @param executor
     * @return
     */
    protected override def reader(executor:Executor) : DataFrameReader = {
        val reader = executor.spark.read
            .format(format)
            .options(options)

        // Apply explicit schema, and load dataFrame
        inputSchema.foreach(s => reader.schema(s))

        reader
    }

    /**
     * Creates a DataFrameReader with bells and whistles configured from the specification
     *
     * @param executor
     * @return
     */
    protected override def writer(executor:Executor, df:DataFrame) : DataFrameWriter[Row] = {
        applyOutputSchema(executor, df).write
            .format(format)
            .options(options)
    }
}



class GenericRelationSpec extends RelationSpec with SchemaRelationSpec {
    @JsonProperty(value="format", required = true) private var format: String = "csv"

    /**
     * Creates the instance of the specified Relation with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): GenericRelation = {
        GenericRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            context.evaluate(format)
        )
    }
}
