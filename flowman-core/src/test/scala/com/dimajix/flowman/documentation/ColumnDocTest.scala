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

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.NullType
import com.dimajix.flowman.types.StringType


class ColumnDocTest extends AnyFlatSpec with MockFactory with Matchers {
    "A ColumnDoc" should "support merge" in {
        val doc1 = ColumnDoc(
            None,
            Field("col1", NullType, description = Some("Some desc 1")),
            children = Seq(
                ColumnDoc(None, Field("child1", StringType, description = Some("Some child desc 1"))),
                ColumnDoc(None, Field("child2", StringType, description = Some("Some child desc 1")))
            )
        )
        val doc2 = ColumnDoc(
            None,
            Field("col2", DoubleType, description = Some("Some desc 2")),
            children = Seq(
                ColumnDoc(None, Field("child2", NullType, description = Some("Some override child desc 1"))),
                ColumnDoc(None, Field("child3", NullType, description = Some("Some override child desc 1")))
            )
        )

        val result = doc1.merge(doc2)

        result should be (ColumnDoc(
            None,
            Field("col1", DoubleType, description = Some("Some desc 2")),
            children = Seq(
                ColumnDoc(None, Field("child1", StringType, description = Some("Some child desc 1"))),
                ColumnDoc(None, Field("child2", StringType, description = Some("Some override child desc 1"))),
                ColumnDoc(None, Field("child3", NullType, description = Some("Some override child desc 1")))
            )
        ))
    }

    it should "support reparent" in {
        val doc1 = ColumnDoc(
            None,
            Field("col1", NullType, description = Some("Some desc 1")),
            children = Seq(
                ColumnDoc(None, Field("child1", StringType, description = Some("Some child desc 1"))),
                ColumnDoc(None, Field("child2", StringType, description = Some("Some child desc 1")))
            )
        )
        val parent = SchemaDoc(None)

        val result = doc1.reparent(parent.reference)

        result should be (ColumnDoc(
            Some(parent.reference),
            Field("col1", NullType, description = Some("Some desc 1")),
            children = Seq(
                ColumnDoc(Some(ColumnReference(Some(parent.reference), "col1")), Field("child1", StringType, description = Some("Some child desc 1"))),
                ColumnDoc(Some(ColumnReference(Some(parent.reference), "col1")), Field("child2", StringType, description = Some("Some child desc 1")))
            )
        ))
    }

    it should "support sql with no parent" in {
        val doc = ColumnDoc(
            None,
            Field("col1", NullType, description = Some("Some desc 1"))
        )
        val doc2 = doc.copy(children = Seq(
            ColumnDoc(Some(doc.reference), Field("child1", StringType)),
            ColumnDoc(Some(doc.reference), Field("child2", StringType))
        ))

        doc2.reference.sql should be ("col1")
        doc2.children(0).reference.sql should be ("col1.child1")
        doc2.children(1).reference.sql should be ("col1.child2")
    }

    it should "support sql with a relation parent" in {
        val rel = mock[Relation]
        (rel.kind _).expects().returns("hive")
        (rel.identifier _).expects().returns(RelationIdentifier("rel1"))
        val doc0 = RelationDoc(
            None,
            Some(rel)
        )
        val doc1 = SchemaDoc(
            Some(doc0.reference)
        )
        val doc2 = ColumnDoc(
            Some(doc1.reference),
            Field("col1", NullType, description = Some("Some desc 1"))
        )
        val doc2p = doc2.copy(children = Seq(
            ColumnDoc(Some(doc2.reference), Field("child1", StringType)),
            ColumnDoc(Some(doc2.reference), Field("child2", StringType))
        ))
        val doc1p = doc1.copy(
            columns = Seq(doc2p)
        )
        val doc0p = doc0.copy(
            schema = Some(doc1p)
        )

        doc0p.schema.get.columns(0).reference.sql should be ("[rel1].col1")
        doc0p.schema.get.columns(0).children(0).reference.sql should be ("[rel1].col1.child1")
        doc0p.schema.get.columns(0).children(1).reference.sql should be ("[rel1].col1.child2")
    }

    it should "support sql with a relation parent and a project" in {
        val rel = mock[Relation]
        (rel.kind _).expects().returns("hive")
        (rel.identifier _).expects().returns(RelationIdentifier("project/rel1"))
        val doc0 = ProjectDoc("project")
        val doc1 = RelationDoc(
            Some(doc0.reference),
            Some(rel)
        )
        val doc2 = SchemaDoc(
            Some(doc1.reference)
        )
        val doc3 = ColumnDoc(
            Some(doc2.reference),
            Field("col1", NullType, description = Some("Some desc 1"))
        )
        val doc3p = doc3.copy(children = Seq(
            ColumnDoc(Some(doc3.reference), Field("child1", StringType)),
            ColumnDoc(Some(doc3.reference), Field("child2", StringType))
        ))
        val doc2p = doc2.copy(
            columns = Seq(doc3p)
        )
        val doc1p = doc1.copy(
            schema = Some(doc2p)
        )
        val doc0p = doc0.copy(
            relations = Seq(doc1p)
        )

        doc0p.relations(0).schema.get.columns(0).reference.sql should be ("[project/rel1].col1")
        doc0p.relations(0).schema.get.columns(0).children(0).reference.sql should be ("[project/rel1].col1.child1")
        doc0p.relations(0).schema.get.columns(0).children(1).reference.sql should be ("[project/rel1].col1.child2")
    }

    it should "support sql with a mapping parent and a no project" in {
        val map = mock[Mapping]
        (map.kind _).expects().returns("sql")
        (map.identifier _).expects().returns(MappingIdentifier("project/map1"))
        val doc1 = MappingDoc(
            None,
            Some(map)
        )
        val doc2 = MappingOutputDoc(
            Some(doc1.reference),
            MappingOutputIdentifier("project/map1:lala")
        )
        val doc3 = SchemaDoc(
            Some(doc2.reference)
        )
        val doc4 = ColumnDoc(
            Some(doc3.reference),
            Field("col1", NullType, description = Some("Some desc 1"))
        )
        val doc4p = doc4.copy(children = Seq(
            ColumnDoc(Some(doc4.reference), Field("child1", StringType)),
            ColumnDoc(Some(doc4.reference), Field("child2", StringType))
        ))
        val doc3p = doc3.copy(
            columns = Seq(doc4p)
        )
        val doc2p = doc2.copy(
            schema = Some(doc3p)
        )
        val doc1p = doc1.copy(
            outputs = Seq(doc2p)
        )

        doc1p.outputs(0).schema.get.columns(0).reference.sql should be ("[map1:lala].col1")
        doc1p.outputs(0).schema.get.columns(0).children(0).reference.sql should be ("[map1:lala].col1.child1")
        doc1p.outputs(0).schema.get.columns(0).children(1).reference.sql should be ("[map1:lala].col1.child2")
    }

    it should "support sql with a mapping parent and a project" in {
        val map = mock[Mapping]
        (map.kind _).expects().returns("sql")
        (map.identifier _).expects().returns(MappingIdentifier("project/map1"))
        val doc0 = ProjectDoc("project")
        val doc1 = MappingDoc(
            Some(doc0.reference),
            Some(map)
        )
        val doc2 = MappingOutputDoc(
            Some(doc1.reference),
            MappingOutputIdentifier("project/map1:lala")
        )
        val doc3 = SchemaDoc(
            Some(doc2.reference)
        )
        val doc4 = ColumnDoc(
            Some(doc3.reference),
            Field("col1", NullType, description = Some("Some desc 1"))
        )
        val doc4p = doc4.copy(children = Seq(
            ColumnDoc(Some(doc4.reference), Field("child1", StringType)),
            ColumnDoc(Some(doc4.reference), Field("child2", StringType))
        ))
        val doc3p = doc3.copy(
            columns = Seq(doc4p)
        )
        val doc2p = doc2.copy(
            schema = Some(doc3p)
        )
        val doc1p = doc1.copy(
            outputs = Seq(doc2p)
        )
        val doc0p = doc0.copy(
            mappings = Seq(doc1p)
        )

        doc0p.mappings(0).outputs(0).schema.get.columns(0).reference.sql should be ("project/[map1:lala].col1")
        doc0p.mappings(0).outputs(0).schema.get.columns(0).children(0).reference.sql should be ("project/[map1:lala].col1.child1")
        doc0p.mappings(0).outputs(0).schema.get.columns(0).children(1).reference.sql should be ("project/[map1:lala].col1.child2")
    }
}
