/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.documentation

import scala.collection.JavaConverters._

import com.dimajix.flowman.model.ResourceIdentifierWrapper


final case class TestResultWrapper(result:TestResult) {
    override def toString: String = result.status.toString

    def getReference() : String = result.reference.toString
    def getDescription() : String = result.description.getOrElse("")
    def getStatus() : String = result.status.toString
}


final case class ColumnTestWrapper(test:ColumnTest) {
    override def toString: String = test.name

    def getReference() : String = test.reference.toString
    def getName() : String = test.name
    def getDescription() : String = test.description.getOrElse("")
    def getResult() : TestResultWrapper = test.result.map(TestResultWrapper).orNull
    def getStatus() : String = test.result.map(_.status.toString).getOrElse("NOT_RUN")
}


final case class ColumnDocWrapper(column:ColumnDoc) {
    override def toString: String = column.name

    def getReference() : String = column.reference.toString
    def getName() : String = column.name
    def getNullable() : Boolean = column.nullable
    def getType() : String = column.typeName
    def getSqlType() : String = column.sqlType
    def getSparkType() : String = column.sparkType
    def getCatalogType() : String = column.catalogType
    def getDescription() : String = column.description.getOrElse("")
    def getColumns() : java.util.List[ColumnDocWrapper] = column.children.map(ColumnDocWrapper).asJava
    def getTests() : java.util.List[ColumnTestWrapper] = column.tests.map(ColumnTestWrapper).asJava
}


final case class SchemaDocWrapper(schema:SchemaDoc) {
    def getReference() : String = schema.reference.toString
    def getDescription() : String = schema.description.getOrElse("")
    def getColumns() : java.util.List[ColumnDocWrapper] = schema.columns.map(ColumnDocWrapper).asJava
}


final case class MappingOutputDocWrapper(output:MappingOutputDoc) {
    override def toString: String = output.identifier.toString

    def getReference() : String = output.reference.toString
    def getIdentifier() : String = output.identifier.toString
    def getProject() : String = output.identifier.project.getOrElse("")
    def getName() : String = output.identifier.output
    def getMapping() : String = output.identifier.name
    def getOutput() : String = output.identifier.output
    def getDescription() : String = output.description.getOrElse("")
    def getSchema() : SchemaDocWrapper = output.schema.map(SchemaDocWrapper).orNull
}


final case class MappingDocWrapper(mapping:MappingDoc) {
    override def toString: String = mapping.identifier.toString

    def getReference() : String = mapping.reference.toString
    def getIdentifier() : String = mapping.identifier.toString
    def getProject() : String = mapping.identifier.project.getOrElse("")
    def getName() : String = mapping.identifier.name
    def getDescription() : String = mapping.description.getOrElse("")
    def getInputs() : java.util.List[String] = mapping.inputs.map(_.toString).asJava
    def getOutputs() : java.util.List[MappingOutputDocWrapper] = mapping.outputs.map(MappingOutputDocWrapper).asJava
}


final case class RelationDocWrapper(relation:RelationDoc) {
    override def toString: String = relation.identifier.toString

    def getReference() : String = relation.reference.toString
    def getIdentifier() : String = relation.identifier.toString
    def getProject() : String = relation.identifier.project.getOrElse("")
    def getName() : String = relation.identifier.name
    def getDescription() : String = relation.description.getOrElse("")
    def getSchema() : SchemaDocWrapper = relation.schema.map(SchemaDocWrapper).orNull
    def getInputs() : java.util.List[String] = relation.inputs.map(_.toString).asJava
    def getResources() : java.util.List[ResourceIdentifierWrapper] = relation.provides.map(ResourceIdentifierWrapper).asJava
}


final case class TargetPhaseDocWrapper(phase:TargetPhaseDoc) {
    override def toString: String = phase.phase.upper

    def getReference() : String = phase.reference.toString
    def getName() : String = phase.phase.upper
    def getDescription() : String = phase.description.getOrElse("")
}


final case class TargetDocWrapper(target:TargetDoc) {
    override def toString: String = target.identifier.toString

    def getReference() : String = target.reference.toString
    def getIdentifier() : String = target.identifier.toString
    def getProject() : String = target.identifier.project.getOrElse("")
    def getName() : String = target.identifier.name
    def getDescription() : String = target.description.getOrElse("")
    def getPhases() : java.util.List[TargetPhaseDocWrapper] = target.phases.map(TargetPhaseDocWrapper).asJava

    def getOutputs() : java.util.List[String] = target.outputs.map(_.toString).asJava
    def getInputs() : java.util.List[String] = target.inputs.map(_.toString).asJava
}


final case class ProjectDocWrapper(project:ProjectDoc) {
    override def toString: String = project.name

    def getName() : String = project.name
    def getVersion() : String = project.version.getOrElse("")
    def getDescription() : String = project.description.getOrElse("")

    def getMappings() : java.util.List[MappingDocWrapper] = project.mappings.values.map(MappingDocWrapper).toSeq.asJava
    def getRelations() : java.util.List[RelationDocWrapper] = project.relations.values.map(RelationDocWrapper).toSeq.asJava
    def getTargets() : java.util.List[TargetDocWrapper] = project.targets.values.map(TargetDocWrapper).toSeq.asJava
}
