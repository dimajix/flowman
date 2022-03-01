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

import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.FieldValue


object RelationReference {
    def of(parent:Reference, identifier:RelationIdentifier) : RelationReference = {
        identifier.project match {
            case None => RelationReference(Some(parent), identifier.name)
            case Some(project) => RelationReference(Some(ProjectReference(project)), identifier.name)
        }
    }
}
final case class RelationReference(
    parent:Option[Reference],
    name:String
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/relation=" + name
            case None => name
        }
    }
    override def kind : String = "relation"

    def sql : String = {
        parent match {
            case Some(ProjectReference(project)) => project + "/" + name
            case _ => name
        }
    }
}


final case class RelationDoc(
    parent:Option[Reference],
    identifier:RelationIdentifier,
    description:Option[String] = None,
    schema:Option[SchemaDoc] = None,
    inputs:Seq[Reference] = Seq(),
    provides:Seq[ResourceIdentifier] = Seq(),
    requires:Seq[ResourceIdentifier] = Seq(),
    sources:Seq[ResourceIdentifier] = Seq(),
    partitions:Map[String,FieldValue] = Map()
) extends EntityDoc {
    override def reference: RelationReference = RelationReference(parent, identifier.name)
    override def fragments: Seq[Fragment] = schema.toSeq
    override def reparent(parent: Reference): RelationDoc = {
        val ref = RelationReference(Some(parent), identifier.name)
        copy(
            parent = Some(parent),
            schema = schema.map(_.reparent(ref))
        )
    }

    /**
     * Merge this schema documentation with another relation documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:Option[RelationDoc]) : RelationDoc = other.map(merge).getOrElse(this)

    /**
     * Merge this schema documentation with another relation documentation. Note that while documentation attributes
     * of [[other]] have a higher priority than those of the instance itself, the parent of itself has higher priority
     * than the one of [[other]]. This allows for a simply information overlay mechanism.
     * @param other
     */
    def merge(other:RelationDoc) : RelationDoc = {
        val id = if (identifier.isEmpty) other.identifier else identifier
        val desc = other.description.orElse(this.description)
        val schm = schema.map(_.merge(other.schema)).orElse(other.schema)
        val prov = provides.toSet ++ other.provides.toSet
        val reqs = requires.toSet ++ other.requires.toSet
        val srcs = sources.toSet ++ other.sources.toSet
        val result = copy(identifier=id, description=desc, schema=schm, provides=prov.toSeq, requires=reqs.toSeq, sources=srcs.toSeq)
        parent.orElse(other.parent)
            .map(result.reparent)
            .getOrElse(result)
    }
}
