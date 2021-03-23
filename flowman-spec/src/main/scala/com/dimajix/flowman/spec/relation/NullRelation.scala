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

package com.dimajix.flowman.spec.relation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.PartitionedRelation
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.SchemaRelation
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue


case class NullRelation(
    override val instanceProperties:Relation.Properties,
    override val schema:Option[Schema] = None,
    override val partitions: Seq[PartitionField] = Seq()
) extends BaseRelation with SchemaRelation with PartitionedRelation {
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
      * @param schema
      * @param partitions
      * @return
      */
    override def read(execution:Execution, schema:Option[StructType], partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        require(execution != null)
        require(schema != null)
        require(partitions != null)

        val readSchema = schema.orElse(inputSchema)
        if (readSchema.isEmpty)
            throw new IllegalArgumentException("Null relation either needs own schema or a desired input schema")

        val rdd = execution.spark.sparkContext.emptyRDD[Row]
        execution.spark.createDataFrame(rdd, readSchema.get)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param execution
      * @param df
      * @param partition
      */
    override def write(execution:Execution, df:DataFrame, partition:Map[String,SingleValue], mode:OutputMode) : Unit = {
        require(execution != null)
        require(partition != null)

        // Force materialization of all records
        df.count()
    }

    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        require(execution != null)
    }


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
      * Returns true if the relation already exists, otherwise it needs to be created prior usage
     *
     * @param execution
      * @return
      */
    override def exists(execution:Execution) : Trilean = true

    override def create(execution: Execution, ifNotExists:Boolean=false): Unit = {
        require(execution != null)
    }
    override def destroy(execution: Execution, ifExists:Boolean=false): Unit = {
        require(execution != null)
    }
    override def migrate(execution: Execution): Unit = {
        require(execution != null)
    }

    /**
     * Creates a Spark schema from the list of fields. This JDBC implementation will add partition columns, since
     * these are required for reading.
     * @return
     */
    override protected def inputSchema : Option[StructType] = {
        schema.map(s => StructType(s.fields.map(_.sparkField) ++ partitions.map(_.sparkField)))
    }

    /**
     * Creates a Spark schema from the list of fields. The list is used for output operations, i.e. for writing.
     * This JDBC implementation will add partition columns, since these are required for writing.
     * @return
     */
    override protected def outputSchema(execution:Execution) : Option[StructType] = {
        schema.map(s => StructType(s.fields.map(_.sparkField) ++ partitions.map(_.sparkField)))
    }
}



class NullRelationSpec extends RelationSpec with SchemaRelationSpec with PartitionedRelationSpec {
    /**
      * Creates the instance of the specified Relation with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): NullRelation = {
        NullRelation(
            instanceProperties(context),
            schema.map(_.instantiate(context)),
            partitions.map(_.instantiate(context))
        )
    }
}
