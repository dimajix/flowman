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

import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier


final case class MappingOutputReference(
    override val parent:Option[Reference],
    name:String
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/output=" + name
            case None => name
        }
    }
    override def kind : String = "mapping_output"

    def sql : String = {
        parent match {
            case Some(MappingReference(Some(ProjectReference(project)), mapping)) => s"$project/[$mapping:$name]"
            case Some(p:MappingReference) => s"[${p.sql}:$name]"
            case _ => s"[:$name]"
        }
    }
}


final case class MappingOutputDoc(
    parent:Some[Reference],
    identifier: MappingOutputIdentifier,
    description: Option[String] = None,
    schema:Option[SchemaDoc] = None
) extends Fragment {
    override def reference: Reference = MappingOutputReference(parent, identifier.output)
    override def fragments: Seq[Fragment] = schema.toSeq
    override def reparent(parent: Reference): MappingOutputDoc = {
        val ref = MappingOutputReference(Some(parent), identifier.output)
        copy(
            parent=Some(parent),
            schema=schema.map(_.reparent(ref))
        )
    }

    /**
     * Returns the name of the project of the mapping of this output
     * @return
     */
    def project : Option[String] = identifier.project

    /**
     * Returns the mapping identifier of this output
     * @return
     */
    def mapping : MappingIdentifier = identifier.mapping

    /**
     * Returns the name of the output
     * @return
     */
    def name : String = identifier.output

    /**
     * Merge this schema documentation with another mapping documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:Option[MappingOutputDoc]) : MappingOutputDoc = other.map(merge).getOrElse(this)

    /**
     * Merge this schema documentation with another mapping documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:MappingOutputDoc) : MappingOutputDoc = {
        val id = if (identifier.mapping.isEmpty) other.identifier else identifier
        val desc = other.description.orElse(this.description)
        val schm = schema.map(_.merge(other.schema)).orElse(other.schema)
        val result = copy(identifier=id, description=desc, schema=schm)
        parent.orElse(other.parent)
            .map(result.reparent)
            .getOrElse(result)
    }
}


object MappingReference {
    def of(parent:Reference, identifier:MappingIdentifier) : MappingReference = {
        identifier.project match {
            case None => MappingReference(Some(parent), identifier.name)
            case Some(project) => MappingReference(Some(ProjectReference(project)), identifier.name)
        }
    }
}
final case class MappingReference(
    override val parent:Option[Reference] = None,
    name:String
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/mapping=" + name
            case None => name
        }
    }
    override def kind: String = "mapping"

    def sql : String = {
        parent match {
            case Some(ProjectReference(project)) => project + "/" + name
            case _ => name
        }
    }
}


final case class MappingDoc(
    parent:Option[Reference] = None,
    mapping:Option[Mapping] = None,
    description:Option[String] = None,
    inputs:Seq[Reference] = Seq.empty,
    outputs:Seq[MappingOutputDoc] = Seq.empty
) extends EntityDoc {
    override def reference: MappingReference = MappingReference(parent, name)
    override def fragments: Seq[Fragment] = outputs
    override def reparent(parent: Reference): MappingDoc = {
        val ref = MappingReference(Some(parent), name)
        copy(
            parent=Some(parent),
            outputs=outputs.map(_.reparent(ref))
        )
    }

    /**
     * Returns the name of the project of this mapping
     * @return
     */
    def project : Option[String] = mapping.flatMap(_.project.map(_.name))

    /**
     * Returns the name of this mapping
     * @return
     */
    def name : String = mapping.map(_.name).getOrElse("")

    def identifier : MappingIdentifier = mapping.map(_.identifier).getOrElse(MappingIdentifier.empty)

    /**
     * Merge this schema documentation with another mapping documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:Option[MappingDoc]) : MappingDoc = other.map(merge).getOrElse(this)

    /**
     * Merge this schema documentation with another mapping documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:MappingDoc) : MappingDoc = {
        val map = mapping.orElse(other.mapping)
        val desc = other.description.orElse(this.description)
        val in = inputs.toSet ++ other.inputs.toSet
        val out = outputs.map { out =>
                out.merge(other.outputs.find(_.identifier.output == out.identifier.output))
            } ++
            other.outputs.filter(out => !outputs.exists(_.identifier.output == out.identifier.output))
        val result = copy(mapping=map, description=desc, inputs=in.toSeq, outputs=out)
        parent.orElse(other.parent)
            .map(result.reparent)
            .getOrElse(result)
    }
}
