/*
 * Copyright 2021 Kaya Kupferschmidt
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

import org.apache.spark.sql.Row
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.types.ArrayRecord
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class MockRelationTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession{
    "A MockRelation" should "be parsable" in {
        val spec =
            """
              |relations:
              |  empty:
              |    kind: null
              |    schema:
              |      kind: embedded
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |
              |  mock:
              |    kind: mock
              |    relation: empty
              |    records:
              |      - ["a",12,3]
              |      - [cat,"",7]
              |      - [dog,null,8]
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("mock")).asInstanceOf[MockRelation]
        relation shouldBe a[MockRelation]

        relation.category should be ("relation")
        relation.kind should be ("mock")
        relation.relation should be (RelationIdentifier("empty"))
        relation.records should be (Seq(
            ArrayRecord("a","12","3"),
            ArrayRecord("cat","","7"),
            ArrayRecord("dog",null,"8")
        ))
    }

    it should "support create, write and destroy" in {
        val baseRelationTemplate = mock[Prototype[Relation]]
        val mockRelationTemplate = mock[Prototype[Relation]]

        val project = Project(
            "my_project",
            relations = Map(
                "base" -> baseRelationTemplate,
                "mock" -> mockRelationTemplate
            )
        )

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val mockRelation = MockRelation(
            Relation.Properties(context, "mock"),
            RelationIdentifier("base")
        )

        (mockRelationTemplate.instantiate _).expects(context).returns(mockRelation)
        val relation = context.getRelation(RelationIdentifier("mock"))
        relation shouldBe a[MockRelation]
        relation.category should be ("relation")

        relation.requires should be (Set())
        relation.provides should be (Set())
        relation.resources(Map()) should be (Set())

        // Initial state
        relation.exists(executor) should be (No)
        relation.loaded(executor, Map()) should be (No)

        // Create
        relation.create(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (No)

        // Write
        relation.write(executor, spark.emptyDataFrame)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (Yes)

        // Truncate
        relation.truncate(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor, Map()) should be (No)

        // Destroy
        relation.destroy(executor)
        relation.exists(executor) should be (No)
        relation.loaded(executor, Map()) should be (No)
    }

    it should "read empty DataFrames" in {
        val baseRelationTemplate = mock[Prototype[Relation]]
        val baseRelation = mock[Relation]
        val mockRelationTemplate = mock[Prototype[Relation]]

        val project = Project(
            "my_project",
            relations = Map(
                "base" -> baseRelationTemplate,
                "mock" -> mockRelationTemplate
            )
        )

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val mockRelation = MockRelation(
            Relation.Properties(context, "mock"),
            RelationIdentifier("base")
        )
        val schema = EmbeddedSchema(
            Schema.Properties(context),
            fields = Seq(
                Field("str_col", StringType),
                Field("int_col", IntegerType)
            )
        )

        (mockRelationTemplate.instantiate _).expects(context).returns(mockRelation)
        val relation = context.getRelation(RelationIdentifier("mock"))

        (baseRelationTemplate.instantiate _).expects(context).returns(baseRelation)
        (baseRelation.schema _).expects().atLeastOnce().returns(Some(schema))
        relation.schema should be (Some(schema))

        (baseRelation.partitions _).expects().atLeastOnce().returns(Seq())
        relation.partitions should be (Seq())

        (baseRelation.fields _).expects().atLeastOnce().returns(schema.fields)
        relation.fields should be (schema.fields)
        relation.describe(executor) should be (new StructType(schema.fields))

        val df1 = relation.read(executor, None, Map())
        df1.schema should be (new StructType(schema.fields).sparkType)
        df1.count() should be (0)

        val readSchema = StructType(Seq(Field("int_col", IntegerType)))
        val df2 = relation.read(executor, Some(readSchema.sparkType))
        df2.schema should be (readSchema.sparkType)
        df2.count() should be (0)
    }

    it should "work nicely with overrides" in {
        val baseRelationTemplate = mock[Prototype[Relation]]
        val baseRelation = mock[Relation]
        val mockRelationTemplate = mock[Prototype[Relation]]

        val project = Project(
            "my_project",
            relations = Map(
                "base" -> baseRelationTemplate
            )
        )

        val session = Session.builder().withSparkSession(spark).build()
        val context = RootContext.builder(session.context)
            .overrideRelations(Map(
                RelationIdentifier("base", "my_project") -> mockRelationTemplate
            ))
            .build()
            .getProjectContext(project)
        val executor = session.execution

        val mockRelation = MockRelation(
            Relation.Properties(context, "base"),
            RelationIdentifier("base")
        )
        val schema = EmbeddedSchema(
            Schema.Properties(context),
            fields = Seq(
                Field("str_col", StringType),
                Field("int_col", IntegerType)
            )
        )

        (mockRelationTemplate.instantiate _).expects(context).returns(mockRelation)
        val relation = context.getRelation(RelationIdentifier("base"))

        (baseRelationTemplate.instantiate _).expects(context).returns(baseRelation)
        (baseRelation.schema _).expects().atLeastOnce().returns(Some(schema))
        relation.schema should be (Some(schema))

        (baseRelation.partitions _).expects().atLeastOnce().returns(Seq())
        relation.partitions should be (Seq())

        (baseRelation.fields _).expects().atLeastOnce().returns(schema.fields)
        relation.fields should be (schema.fields)
        relation.describe(executor) should be (new StructType(schema.fields))
    }

    it should "return provided records as a DataFrame" in {
        val baseRelationTemplate = mock[Prototype[Relation]]
        val baseRelation = mock[Relation]
        val mockRelationTemplate = mock[Prototype[Relation]]

        val project = Project(
            "my_project",
            relations = Map(
                "base" -> baseRelationTemplate,
                "mock" -> mockRelationTemplate
            )
        )

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val mockRelation = MockRelation(
            Relation.Properties(context, "mock"),
            RelationIdentifier("base"),
            Seq(
                ArrayRecord("lala","12"),
                ArrayRecord("lolo","13"),
                ArrayRecord("",null)
            )
        )
        val schema = EmbeddedSchema(
            Schema.Properties(context),
            fields = Seq(
                Field("str_col", StringType),
                Field("int_col", IntegerType)
            )
        )

        (mockRelationTemplate.instantiate _).expects(context).returns(mockRelation)
        val relation = context.getRelation(RelationIdentifier("mock"))

        (baseRelationTemplate.instantiate _).expects(context).returns(baseRelation)
        (baseRelation.schema _).expects().anyNumberOfTimes().returns(Some(schema))
        (baseRelation.partitions _).expects().anyNumberOfTimes().returns(Seq())
        val df = relation.read(executor, None, Map())
        df.schema should be (new StructType(schema.fields).sparkType)
        df.collect() should be (Seq(
            Row("lala", 12),
            Row("lolo", 13),
            Row(null,null)
        ))
    }
}
