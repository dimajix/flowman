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
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.sql.SchemaUtils


case class GenericRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema],
    format:String,
    options:Map[String,String] = Map()
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
     * @param execution
     * @param schema - the schema to read. If none is specified, all available columns will be read
     * @param partitions - List of partitions. If none are specified, all the data will be read
     * @return
     */
    override def read(execution:Execution, schema:Option[StructType], partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(execution != null)
        require(schema != null)
        require(partitions != null)

        logger.info(s"Reading generic relation '$identifier'")

        val data = reader(execution, format, options).load()
        SchemaUtils.applySchema(data, schema)
    }

    /**
     * Writes data into the relation, possibly into a specific partition
     * @param execution
     * @param df - dataframe to write
     * @param partition - destination partition
     */
    override def write(execution:Execution, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode = OutputMode.OVERWRITE) : Unit = {
        require(execution != null)
        require(df != null)
        require(partition != null)

        logger.info(s"Writing generic relation '$identifier' with mode '$mode'")

        writer(execution, df, format, options, mode.batchMode)
            .save()
    }

    /**
     * Returns true if the relation already exists, otherwise it needs to be created prior usage
     * @param execution
     * @return
     */
    override def exists(execution:Execution) : Trilean = Unknown


    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     *
     * @param execution
     * @param partition
     * @return
     */
    override def loaded(execution: Execution, partition: Map[String, SingleValue]): Trilean = Unknown

    /**
     * This method will create the given directory as specified in "location"
     *
     * @param execution
     */
    override def create(execution:Execution, ifNotExists:Boolean=false) : Unit = {}

    /**
     * This will update any existing relation to the specified metadata. Actually for this file based target, the
     * command will precisely do nothing.
     *
     * @param execution
     */
    override def migrate(execution:Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy) : Unit = {}

    /**
     * Removes one or more partitions.
     * @param execution
     * @param partitions
     */
    override def truncate(execution:Execution, partitions:Map[String,FieldValue] = Map()) : Unit = {}

    /**
     * This method will remove the given directory as specified in "location"
     * @param execution
     */
    override def destroy(execution:Execution, ifExists:Boolean) : Unit =  {}
}



class GenericRelationSpec extends RelationSpec with SchemaRelationSpec {
    @JsonProperty(value="format", required = true) private var format: String = "csv"
    @JsonProperty(value="options", required=false) private var options:Map[String,String] = Map()

    /**
     * Creates the instance of the specified Relation with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): GenericRelation = {
        GenericRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            context.evaluate(format),
            context.evaluate(options)
        )
    }
}
