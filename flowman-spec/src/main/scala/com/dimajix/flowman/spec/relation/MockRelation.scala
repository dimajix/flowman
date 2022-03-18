/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.types
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.Record
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.sql.DataFrameBuilder
import com.dimajix.spark.sql.DataFrameUtils
import com.dimajix.spark.sql.SchemaUtils


case class MockRelation(
    override val instanceProperties:Relation.Properties,
    relation: RelationIdentifier,
    records:Seq[Record] = Seq()
) extends BaseRelation with SchemaRelation {
    private lazy val mocked = context.getRelation(relation, false)
    private var _exists = false
    private var _loaded = false

    /**
     * Returns the list of all resources which will be created by this relation.
     *
     * @return
     */
    override def provides: Set[ResourceIdentifier] = Set()

    /**
     * Returns the list of all resources which will be required by this relation for creation.
     *
     * @return
     */
    override def requires: Set[ResourceIdentifier] = Set()

    /**
     * Returns the list of all resources which will are managed by this relation for reading or writing a specific
     * partition. The list will be specifically  created for a specific partition, or for the full relation (when the
     * partition is empty)
     *
     * @param partitions
     * @return
     */
    override def resources(partitions: Map[String, FieldValue]): Set[ResourceIdentifier] = Set()

    /**
     * Reads data from the relation, possibly from specific partitions
     *
     * @param execution
     * @param partitions - List of partitions. If none are specified, all the data will be read
     * @return
     */
    override def read(execution: Execution, partitions: Map[String, FieldValue]): DataFrame = {
        require(execution != null)
        require(partitions != null)

        if (records.nonEmpty) {
            val fullSchema = this.schema.map(s => com.dimajix.flowman.types.StructType(s.fields ++ this.partitions.map(_.field)))
                .getOrElse(throw new IllegalArgumentException("Cannot mock relation with records without schema information"))

            val values = records.map(_.toArray(fullSchema))
            DataFrameBuilder.ofStringValues(execution.spark, values, fullSchema.sparkType)
        }
        else {
            val readSchema = inputSchema
                .getOrElse(throw new IllegalArgumentException("Mock relation either needs own schema or a desired input schema"))

            DataFrameBuilder.ofSchema(execution.spark, readSchema)
        }
    }

    /**
     * Writes data into the relation, possibly into a specific partition
     *
     * @param execution
     * @param df        - dataframe to write
     * @param partition - destination partition
     */
    override def write(execution: Execution, df: DataFrame, partition: Map[String, SingleValue], mode: OutputMode): Unit = {
        require(execution != null)
        require(partition != null)

        // Force materialization of all records
        df.count()

        _exists = true
        _loaded = true
    }

    /**
     * Removes one or more partitions.
     *
     * @param execution
     * @param partitions
     */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        _loaded = false
    }

    /**
     * Returns true if the relation already exists, otherwise it needs to be created prior usage. This refers to
     * the relation itself, not to the data or a specific partition. [[loaded]] should return [[Yes]] after
     * [[[create]] has been called, and it should return [[No]] after [[destroy]] has been called.
     *
     * @param execution
     * @return
     */
    override def exists(execution: Execution): Trilean = _exists

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution, migrationPolicy: MigrationPolicy): Trilean = true

    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     *
     * @param execution
     * @param partition
     * @return
     */
    override def loaded(execution: Execution, partition: Map[String, SingleValue]): Trilean = {
        if (_loaded)
            Yes
        else
            No
    }

    /**
     * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
     * relation will not contain any data, but all metadata will be processed
     *
     * @param execution
     */
    override def create(execution: Execution, ifNotExists: Boolean): Unit = {
        _exists = true
    }

    /**
     * This will delete any physical representation of the relation. Depending on the type only some meta data like
     * a Hive table might be dropped or also the physical files might be deleted
     *
     * @param execution
     */
    override def destroy(execution: Execution, ifExists: Boolean): Unit = {
        _loaded = false
        _exists = false
    }

    /**
     * This will update any existing relation to the specified metadata.
     *
     * @param execution
     */
    override def migrate(execution: Execution, migrationPolicy:MigrationPolicy, migrationStrategy:MigrationStrategy): Unit = {}

    /**
     * Returns the schema of the relation, excluding partition columns
     *
     * @return
     */
    override def schema: Option[Schema] = mocked.schema

    /**
     * Returns the list of partition columns
     *
     * @return
     */
    override def partitions: Seq[PartitionField] = mocked.partitions

    /**
      * Returns a list of fields including the partition columns. This method should not perform any physical schema
      * inference.
      *
      * @return
      */
    override def fields: Seq[Field] = mocked.fields

    /**
     * Returns the schema of the relation. This implementation will *not* simply call the [[describe]] Method
     * of the mocked instance, but it will use the [[fields]] method instead. This ensures that no physical data
     * source is inspected during mocking.
     *
     * @param execution
     * @return
     */
    override def describe(execution: Execution, partitions:Map[String,FieldValue] = Map()): types.StructType = {
        val result = types.StructType(mocked.fields)

        applyDocumentation(result)
    }

    /**
     * Creates a Spark schema from the list of fields. This implementation will add partition columns, since
     * these are part of the specification.
     * @return
     */
    override protected def inputSchema : Option[StructType] = {
        schema.map(s => StructType(s.fields.map(_.sparkField) ++ partitions.map(_.sparkField)))
    }

    /**
     * Creates a Spark schema from the list of fields. The list is used for output operations, i.e. for writing.
     * This implementation will add partition columns, since these are required for writing.
     * @return
     */
    override protected def outputSchema(execution:Execution) : Option[StructType] = {
        schema.map(s => StructType(s.fields.map(_.catalogField) ++ partitions.map(_.catalogField)))
    }
}


class MockRelationSpec extends RelationSpec {
    @JsonProperty(value="relation", required=true) private var relation: Option[String] = None
    @JsonProperty(value="records", required=false) private var records:Seq[Record] = Seq()

    /**
     * Creates the instance of the specified Relation with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context, properties:Option[Relation.Properties] = None): MockRelation = {

        MockRelation(
            instanceProperties(context, properties),
            RelationIdentifier(context.evaluate(relation).getOrElse(name)),
            records.map(_.map(context.evaluate))
        )
    }
}
