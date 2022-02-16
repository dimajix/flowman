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


final case class ReferenceWrapper(reference:Reference) {
    override def toString: String = reference.toString

    def getParent() : ReferenceWrapper = reference.parent.map(ReferenceWrapper).orNull
    def getKind() : String = reference.kind
}


class FragmentWrapper(fragment:Fragment) {
    def getReference() : ReferenceWrapper = ReferenceWrapper(fragment.reference)
    def getParent() : ReferenceWrapper = fragment.parent.map(ReferenceWrapper).orNull
    def getDescription() : String = fragment.description.getOrElse("")
}


final case class TestResultWrapper(result:TestResult) extends FragmentWrapper(result) {
    override def toString: String = result.status.toString

    def getStatus() : String = result.status.toString
}


final case class ColumnTestWrapper(test:ColumnTest) extends FragmentWrapper(test) {
    override def toString: String = test.name

    def getName() : String = test.name
    def getResult() : TestResultWrapper = test.result.map(TestResultWrapper).orNull
    def getStatus() : String = test.result.map(_.status.toString).getOrElse("NOT_RUN")
}


final case class ColumnDocWrapper(column:ColumnDoc) extends FragmentWrapper(column) {
    override def toString: String = column.name

    def getName() : String = column.name
    def getNullable() : Boolean = column.nullable
    def getType() : String = column.typeName
    def getSqlType() : String = column.sqlType
    def getSparkType() : String = column.sparkType
    def getCatalogType() : String = column.catalogType
    def getColumns() : java.util.List[ColumnDocWrapper] = column.children.map(ColumnDocWrapper).asJava
    def getTests() : java.util.List[ColumnTestWrapper] = column.tests.map(ColumnTestWrapper).asJava
}


final case class SchemaDocWrapper(schema:SchemaDoc)  extends FragmentWrapper(schema) {
    def getColumns() : java.util.List[ColumnDocWrapper] = schema.columns.map(ColumnDocWrapper).asJava
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
        val x = project.resolve(reference.reference).map {
            case m:MappingDoc => MappingDocWrapper(m)
            case o:MappingOutputDoc => MappingOutputDocWrapper(o)
            case r:RelationDoc => RelationDocWrapper(r)
            case t:TargetDoc => TargetDocWrapper(t)
            case p:TargetPhaseDoc => TargetPhaseDocWrapper(p)
            case s:SchemaDoc => SchemaDocWrapper(s)
            case t:TestResult => TestResultWrapper(t)
            case c:ColumnDoc => ColumnDocWrapper(c)
            case t:ColumnTest => ColumnTestWrapper(t)
            case f:Fragment => new FragmentWrapper(f)
        }.orNull

        if (x == null)
            println("null")

        x
    }

    def getMappings() : java.util.List[MappingDocWrapper] = project.mappings.values.map(MappingDocWrapper).toSeq.asJava
    def getRelations() : java.util.List[RelationDocWrapper] = project.relations.values.map(RelationDocWrapper).toSeq.asJava
    def getTargets() : java.util.List[TargetDocWrapper] = project.targets.values.map(TargetDocWrapper).toSeq.asJava
}
