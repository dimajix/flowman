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

import com.dimajix.flowman.model.Relation
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
            case Some(ProjectReference(project)) => s"[$project/$name]"
            case _ => s"[$name]"
        }
    }
}


object RelationDoc {
    def apply(
        parent: Option[Reference],
        relation: Option[Relation] = None,
        description: Option[String] = None,
        schema: Option[SchemaDoc] = None,
        inputs: Seq[Reference] = Seq.empty,
        provides: Seq[ResourceIdentifier] = Seq.empty,
        requires: Seq[ResourceIdentifier] = Seq.empty,
        sources: Seq[ResourceIdentifier] = Seq.empty,
        partitions: Map[String, FieldValue] = Map.empty
    ) : RelationDoc = {
        RelationDoc(
            relation,
            parent,
            relation.map(_.kind).getOrElse(""),
            relation.map(_.identifier).getOrElse(RelationIdentifier.empty),
            description,
            schema,
            inputs,
            provides,
            requires,
            sources,
            partitions
        )
    }
}
final case class RelationDoc(
    relation: Option[Relation],
    parent: Option[Reference],
    kind: String,
    identifier: RelationIdentifier,
    description: Option[String],
    schema: Option[SchemaDoc],
    inputs: Seq[Reference],
    provides: Seq[ResourceIdentifier],
    requires: Seq[ResourceIdentifier],
    sources: Seq[ResourceIdentifier],
    partitions: Map[String,FieldValue]
) extends EntityDoc {
    override def reference: RelationReference = RelationReference(parent, name)
    override def fragments: Seq[Fragment] = schema.toSeq
    override def reparent(parent: Reference): RelationDoc = {
        val ref = RelationReference(Some(parent), name)
        copy(
            parent = Some(parent),
            schema = schema.map(_.reparent(ref))
        )
    }
    /**
     * Returns the name of the project of this relation
     * @return
     */
    def project : Option[String] = identifier.project

    override def category: Category = Category.RELATION
    override def name : String = identifier.name

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
        val rel = this.relation.orElse(other.relation)
        val id = if (this.identifier.isEmpty) other.identifier else this.identifier
        val kind = if (this.kind.isEmpty) other.kind else this.kind
        val desc = other.description.orElse(this.description)
        val schm = schema.map(_.merge(other.schema)).orElse(other.schema)
        val prov = provides.toSet ++ other.provides.toSet
        val reqs = requires.toSet ++ other.requires.toSet
        val srcs = sources.toSet ++ other.sources.toSet
        val parts = other.partitions ++ partitions
        val result = copy(relation=rel, kind=kind, identifier=id, description=desc, schema=schm, provides=prov.toSeq, requires=reqs.toSeq, sources=srcs.toSeq, partitions=parts)
        parent.orElse(other.parent)
            .map(result.reparent)
            .getOrElse(result)
    }
}
