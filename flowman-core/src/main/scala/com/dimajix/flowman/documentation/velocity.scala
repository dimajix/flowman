/*
 * Copyright (C) 2022 The Flowman Authors
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


final case class ReferenceWrapper(reference:Reference) {
    override def toString: String = reference.toString

    def getParent() : ReferenceWrapper = reference.parent.map(ReferenceWrapper).orNull
    def getKind() : String = reference.kind
    def getSql() : String = reference match {
        case m:MappingReference => m.sql
        case m:MappingOutputReference => m.sql
        case m:RelationReference => m.sql
        case m:ColumnReference => m.sql
        case m:SchemaReference => m.sql
        case _ => ""
    }
}


class FragmentWrapper(fragment:Fragment) {
    def getReference() : ReferenceWrapper = ReferenceWrapper(fragment.reference)
    def getParent() : ReferenceWrapper = fragment.parent.map(ReferenceWrapper).orNull
    def getDescription() : String = fragment.description.getOrElse("")
}


final case class CheckResultWrapper(result:CheckResult) extends FragmentWrapper(result) {
    override def toString: String = result.status.toString

    def getStatus() : String = result.status.toString
    def getSuccess() : Boolean = result.success
    def getFailure() : Boolean = result.failure
}


final case class ColumnCheckWrapper(check:ColumnCheck) extends FragmentWrapper(check) {
    override def toString: String = check.name

    def getName() : String = check.name
    def getText() : String = check.text
    def getResult() : CheckResultWrapper = check.result.map(CheckResultWrapper).orNull
    def getStatus() : String = check.result.map(_.status.toString).getOrElse("NOT_RUN")
    def getSuccess() : Boolean = check.result.exists(_.success)
    def getFailure() : Boolean = check.result.exists(_.failure)
}


final case class ColumnDocWrapper(column:ColumnDoc) extends FragmentWrapper(column) {
    override def toString: String = column.name

    def getName() : String = column.name
    def getFqName() : String = column.fqName
    def getNullable() : Boolean = column.nullable
    def getType() : String = column.typeName
    def getSqlType() : String = column.sqlType
    def getSparkType() : String = column.sparkType
    def getCatalogType() : String = column.catalogType
    def getIndex() : Int = column.index
    def getColumns() : java.util.List[ColumnDocWrapper] = column.children.map(ColumnDocWrapper).asJava
    def getInputs() : java.util.List[ReferenceWrapper] = column.inputs.map(ReferenceWrapper).asJava
    def getChecks() : java.util.List[ColumnCheckWrapper] = column.checks.map(ColumnCheckWrapper).asJava
}


final case class SchemaCheckWrapper(check:SchemaCheck) extends FragmentWrapper(check) {
    override def toString: String = check.name

    def getName() : String = check.name
    def getText() : String = check.text
    def getResult() : CheckResultWrapper = check.result.map(CheckResultWrapper).orNull
    def getStatus() : String = check.result.map(_.status.toString).getOrElse("NOT_RUN")
    def getSuccess() : Boolean = check.result.exists(_.success)
    def getFailure() : Boolean = check.result.exists(_.failure)
}


final case class SchemaDocWrapper(schema:SchemaDoc)  extends FragmentWrapper(schema) {
    def getColumns() : java.util.List[ColumnDocWrapper] = schema.columns.map(ColumnDocWrapper).asJava
    def getChecks() : java.util.List[SchemaCheckWrapper] = schema.checks.map(SchemaCheckWrapper).asJava
}


final case class MappingOutputDocWrapper(output:MappingOutputDoc) extends FragmentWrapper(output) {
    override def toString: String = output.identifier.toString

    def getIdentifier() : String = output.identifier.toString
    def getProject() : String = output.identifier.project.getOrElse("")
    def getName() : String = output.identifier.output
    def getMapping() : String = output.identifier.name
    def getOutput() : String = output.identifier.output
    def getSchema() : SchemaDocWrapper = output.schema.map(SchemaDocWrapper).orNull
}


final case class MappingDocWrapper(mapping:MappingDoc) extends FragmentWrapper(mapping) {
    override def toString: String = mapping.identifier.toString

    def getIdentifier() : String = mapping.identifier.toString
    def getProject() : String = mapping.identifier.project.getOrElse("")
    def getName() : String = mapping.identifier.name
    def getInputs() : java.util.List[ReferenceWrapper] = mapping.inputs.map(ReferenceWrapper).asJava
    def getOutputs() : java.util.List[MappingOutputDocWrapper] = mapping.outputs.map(MappingOutputDocWrapper).asJava
}


final case class RelationDocWrapper(relation:RelationDoc) extends FragmentWrapper(relation) {
    override def toString: String = relation.identifier.toString

    def getIdentifier() : String = relation.identifier.toString
    def getProject() : String = relation.identifier.project.getOrElse("")
    def getName() : String = relation.identifier.name
    def getSchema() : SchemaDocWrapper = relation.schema.map(SchemaDocWrapper).orNull
    def getInputs() : java.util.List[ReferenceWrapper] = relation.inputs.map(ReferenceWrapper).asJava
    def getResources() : java.util.List[ResourceIdentifierWrapper] = relation.provides.map(ResourceIdentifierWrapper).asJava
    def getDependencies() : java.util.List[ResourceIdentifierWrapper] = relation.requires.map(ResourceIdentifierWrapper).asJava
    def getSources() : java.util.List[ResourceIdentifierWrapper] = relation.sources.map(ResourceIdentifierWrapper).asJava
}


final case class TargetPhaseDocWrapper(phase:TargetPhaseDoc) extends FragmentWrapper(phase) {
    override def toString: String = phase.phase.upper

    def getName() : String = phase.phase.upper
}


final case class TargetDocWrapper(target:TargetDoc) extends FragmentWrapper(target) {
    override def toString: String = target.identifier.toString

    def getIdentifier() : String = target.identifier.toString
    def getProject() : String = target.identifier.project.getOrElse("")
    def getName() : String = target.identifier.name
    def getPhases() : java.util.List[TargetPhaseDocWrapper] = target.phases.map(TargetPhaseDocWrapper).asJava

    def getOutputs() : java.util.List[ReferenceWrapper] = target.outputs.map(ReferenceWrapper).asJava
    def getInputs() : java.util.List[ReferenceWrapper] = target.inputs.map(ReferenceWrapper).asJava
}


final case class ProjectDocWrapper(project:ProjectDoc) extends FragmentWrapper(project) {
    override def toString: String = project.name

    def getName() : String = project.name
    def getVersion() : String = project.version.getOrElse("")

    def resolve(reference:ReferenceWrapper) : FragmentWrapper = {
        val wrapper = project.resolve(reference.reference).map {
            case m:MappingDoc => MappingDocWrapper(m)
            case o:MappingOutputDoc => MappingOutputDocWrapper(o)
            case r:RelationDoc => RelationDocWrapper(r)
            case t:TargetDoc => TargetDocWrapper(t)
            case p:TargetPhaseDoc => TargetPhaseDocWrapper(p)
            case s:SchemaDoc => SchemaDocWrapper(s)
            case s:SchemaCheck => SchemaCheckWrapper(s)
            case t:CheckResult => CheckResultWrapper(t)
            case c:ColumnDoc => ColumnDocWrapper(c)
            case t:ColumnCheck => ColumnCheckWrapper(t)
            case f:Fragment => new FragmentWrapper(f)
        }
        wrapper.orNull
    }

    def getMappings() : java.util.List[MappingDocWrapper] =
        project.mappings
            .sortBy(_.identifier.toString)
            .map(MappingDocWrapper)
            .asJava
    def getRelations() : java.util.List[RelationDocWrapper] =
        project.relations
            .sortBy(_.identifier.toString)
            .map(RelationDocWrapper)
            .asJava
    def getTargets() : java.util.List[TargetDocWrapper] =
        project.targets
            .sortBy(_.identifier.toString)
            .map(TargetDocWrapper)
            .asJava
}
