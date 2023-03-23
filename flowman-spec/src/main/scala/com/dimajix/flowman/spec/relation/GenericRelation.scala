/*
 * Copyright (C) 2020 The Flowman Authors
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

import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.model.SimpleResourceIdentifier
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


final case class GenericRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema],
    format:String,
    options:Map[String,String] = Map()
) extends BaseRelation with SchemaRelation {
    private val resource = SimpleResourceIdentifier("genericRelation", identifier.toString)

    /**
     * Returns the list of all resources which will be created by this relation.
     *
     * @return
     */
    override def provides(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY =>
                Set(resource)
            case Operation.READ | Operation.WRITE  =>
                Set(resource)
        }
    }

    /**
     * Returns the list of all resources which will be required by this relation
     *
     * @return
     */
    override def requires(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] =  {
        op match {
            case Operation.CREATE | Operation.DESTROY => Set.empty
            case Operation.READ | Operation.WRITE  =>
                Set(resource)
        }
    }

    /**
     * Reads data from the relation, possibly from specific partitions
     *
     * @param execution
     * @param schema - the schema to read. If none is specified, all available columns will be read
     * @param partitions - List of partitions. If none are specified, all the data will be read
     * @return
     */
    override def read(execution:Execution, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(execution != null)
        require(schema != null)
        require(partitions != null)

        logger.info(s"Reading generic relation '$identifier'")

        val df = reader(execution, format, options).load()

        // Install callback to refresh DataFrame when data is overwritten
        execution.addResource(resource) {
            df.queryExecution.logical.refresh()
        }

        df
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

        execution.refreshResource(resource)
    }

    /**
     * Returns true if the relation already exists, otherwise it needs to be created prior usage
     * @param execution
     * @return
     */
    override def exists(execution:Execution) : Trilean = Unknown

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution): Trilean = Unknown

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
    override def create(execution:Execution) : Unit = {}

    /**
     * This will update any existing relation to the specified metadata. Actually for this file based target, the
     * command will precisely do nothing.
     *
     * @param execution
     */
    override def migrate(execution:Execution) : Unit = {}

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
    override def destroy(execution:Execution) : Unit =  {}
}



class GenericRelationSpec extends RelationSpec with SchemaRelationSpec {
    @JsonProperty(value="format", required = true) private var format: String = "csv"
    @JsonProperty(value="options", required=false) private var options:Map[String,String] = Map()

    /**
     * Creates the instance of the specified Relation with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Relation.Properties] = None): GenericRelation = {
        GenericRelation(
            instanceProperties(context, properties),
            schema.map(_.instantiate(context)),
            context.evaluate(format),
            context.evaluate(options)
        )
    }
}
