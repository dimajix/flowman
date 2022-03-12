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

package com.dimajix.flowman.spec.template

import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame

import com.dimajix.common.Trilean
import com.dimajix.flowman.documentation.RelationDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.BaseTemplate
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.spec.relation.RelationSpec
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StructType


case class RelationTemplate(
    instanceProperties: Template.Properties,
    parameters: Seq[Template.Parameter],
    spec:Prototype[Relation]
) extends BaseTemplate[Relation] with com.dimajix.flowman.model.RelationTemplate {
    override protected def instantiateInternal(context: Context, name: String): Relation = {
        spec match {
            case spec:RelationSpec =>
                // Temporarily set spec name. Project and namespace are already correctly provided by the context.
                synchronized {
                    val oldName = spec.name
                    spec.name = name
                    val instance = spec.instantiate(context)
                    spec.name = oldName
                    instance
                }
            case spec => spec.instantiate(context)
        }
    }
}

class RelationTemplateSpec extends TemplateSpec {
    @JsonProperty(value="template", required=true) private var spec:RelationSpec = _

    override def instantiate(context: Context): RelationTemplate = {
        RelationTemplate(
            instanceProperties(context),
            parameters.map(_.instantiate(context)),
            spec
        )
    }
}


final case class RelationTemplateInstance(
    instanceProperties: Relation.Properties,
    instance: Relation
) extends AbstractInstance with Relation {
    /**
     * Returns the kind of the resource
     *
     * @return
     */
    override def kind: String = instance.kind

    /**
     * Returns an identifier for this relation
     *
     * @return
     */
    override def identifier: RelationIdentifier = instanceProperties.identifier

    /**
     * Returns a description of the relation
     *
     * @return
     */
    override def description: Option[String] = instanceProperties.description.orElse(instance.description)

    /**
     * Returns a (static) documentation of this relation
     *
     * @return
     */
    override def documentation: Option[RelationDoc] = {
        instanceProperties.documentation
            .map(_.merge(instance.documentation))
            .orElse(instance.documentation)
            .map(_.copy(relation=Some(this)))
    }

    /**
     * Returns the list of all resources which will be created by this relation. This method mainly refers to the
     * CREATE and DESTROY execution phase.
     *
     * @return
     */
    override def provides: Set[ResourceIdentifier] = instance.provides

    /**
     * Returns the list of all resources which will be required by this relation for creation. This method mainly
     * refers to the CREATE and DESTROY execution phase.
     *
     * @return
     */
    override def requires: Set[ResourceIdentifier] = instance.requires

    /**
     * Returns the list of all resources which will are managed by this relation for reading or writing a specific
     * partition. The list will be specifically  created for a specific partition, or for the full relation (when the
     * partition is empty). This method mainly refers to the BUILD and TRUNCATE execution phase.
     *
     * @param partitions
     * @return
     */
    override def resources(partitions: Map[String, FieldValue]): Set[ResourceIdentifier] = instance.resources(partitions)

    /**
     * Returns the schema of the relation, excluding partition columns
     *
     * @return
     */
    override def schema: Option[Schema] = instance.schema

    /**
     * Returns the list of partition columns
     *
     * @return
     */
    override def partitions: Seq[PartitionField] = instance.partitions

    /**
     * Returns a list of fields including the partition columns. This method should not perform any physical schema
     * inference.
     *
     * @return
     */
    override def fields: Seq[Field] = instance.fields

    /**
     * Returns the schema of the relation, either from an explicitly specified schema or by schema inference from
     * the physical source
     *
     * @param execution
     * @param partitions - Optional partition as a hint for schema inference
     * @return
     */
    override def describe(execution: Execution, partitions: Map[String, FieldValue]): StructType = instance.describe(execution, partitions)

    /**
     * Reads data from the relation, possibly from specific partitions
     *
     * @param execution
     * @param partitions - List of partitions. If none are specified, all the data will be read
     * @return
     */
    override def read(execution: Execution, partitions: Map[String, FieldValue]): DataFrame = instance.read(execution, partitions)

    /**
     * Writes data into the relation, possibly into a specific partition
     *
     * @param execution
     * @param df        - dataframe to write
     * @param partition - destination partition
     */
    override def write(execution: Execution, df: DataFrame, partition: Map[String, SingleValue], mode: OutputMode): Unit = instance.write(execution, df, partition, mode)

    /**
     * Removes one or more partitions.
     *
     * @param execution
     * @param partitions
     */
    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = instance.truncate(execution, partitions)

    /**
     * Returns true if the relation already exists, otherwise it needs to be created prior usage. This refers to
     * the relation itself, not to the data or a specific partition. [[loaded]] should return [[Yes]] after
     * [[[create]] has been called, and it should return [[No]] after [[destroy]] has been called.
     *
     * @param execution
     * @return
     */
    override def exists(execution: Execution): Trilean = instance.exists(execution)

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution, migrationPolicy: MigrationPolicy): Trilean = instance.conforms(execution, migrationPolicy)

    /**
     * Returns true if the target partition exists and contains valid data. Absence of a partition indicates that a
     * [[write]] is required for getting up-to-date contents. A [[write]] with output mode
     * [[OutputMode.ERROR_IF_EXISTS]] then should not throw an error but create the corresponding partition
     *
     * @param execution
     * @param partition
     * @return
     */
    override def loaded(execution: Execution, partition: Map[String, SingleValue]): Trilean = instance.loaded(execution, partition)

    /**
     * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
     * relation will not contain any data, but all metadata will be processed
     *
     * @param execution
     */
    override def create(execution: Execution, ifNotExists: Boolean): Unit = instance.create(execution, ifNotExists)

    /**
     * This will delete any physical representation of the relation. Depending on the type only some meta data like
     * a Hive table might be dropped or also the physical files might be deleted
 *
     * @param execution
     */
    override def destroy(execution: Execution, ifExists: Boolean): Unit = instance.destroy(execution, ifExists)

    /**
     * This will update any existing relation to the specified metadata.
     *
     * @param execution
     */
    override def migrate(execution: Execution, migrationPolicy: MigrationPolicy, migrationStrategy: MigrationStrategy): Unit = instance.migrate(execution, migrationPolicy, migrationStrategy)

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker: Linker): Unit = instance.link(linker)
}


class RelationTemplateInstanceSpec extends RelationSpec {
    @JsonIgnore
    private[spec] var args:Map[String,String] = Map()

    @JsonAnySetter
    private def setArg(name:String, value:String) : Unit = {
        args = args.updated(name, value)
    }

    override def instantiate(context: Context): Relation = {
        // get template name from member "kind"
        // Lookup template in context
        val identifier = TemplateIdentifier(kind.stripPrefix("template/"))
        val template = context.getTemplate(identifier).asInstanceOf[RelationTemplate]

        // parse args
        val parsedArgs = template.arguments(context.evaluate(args))
        val instance = template.instantiate(context, name, parsedArgs)

        RelationTemplateInstance(
            instanceProperties(context),
            instance
        )
    }
}
