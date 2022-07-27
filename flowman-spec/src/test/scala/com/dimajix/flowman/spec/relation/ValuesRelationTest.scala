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

import com.dimajix.common.Yes
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types.ArrayRecord
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class ValuesRelationTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The ValuesRelation" should "be parseable with a schema" in {
        val spec =
            """
              |relations:
              |  fake:
              |    kind: values
              |    records:
              |      - ["a",12,3]
              |      - [cat,"",7]
              |      - [dog,null,8]
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("fake")).asInstanceOf[ValuesRelation]
        relation shouldBe a[ValuesRelation]

        relation.category should be (Category.RELATION)
        relation.kind should be ("values")
        relation.requires(Operation.CREATE) should be (Set.empty)
        relation.provides(Operation.CREATE) should be (Set.empty)
        relation.requires(Operation.READ) should be (Set.empty)
        relation.provides(Operation.READ) should be (Set.empty)
        relation.requires(Operation.WRITE) should be (Set.empty)
        relation.provides(Operation.WRITE) should be (Set.empty)
        relation.identifier should be (RelationIdentifier("project/fake"))
        relation.fields should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))
        relation.records should be (Seq(
            ArrayRecord("a","12","3"),
            ArrayRecord("cat","","7"),
            ArrayRecord("dog",null,"8")
        ))
    }

    it should "be parseable with columns" in {
        val spec =
            """
              |relations:
              |  fake:
              |    kind: values
              |    records:
              |      - ["a",12,3]
              |      - [cat,"",7]
              |      - [dog,null,8]
              |    columns:
              |      str_col: string
              |      int_col: integer
              |      some_col: string
              |      other_col: string
              |      last_col: string
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("fake")).asInstanceOf[ValuesRelation]
        relation shouldBe a[ValuesRelation]

        relation.category should be (Category.RELATION)
        relation.kind should be ("values")
        relation.provides(Operation.CREATE) should be (Set())
        relation.requires(Operation.CREATE) should be (Set())
        relation.provides(Operation.READ) should be (Set())
        relation.requires(Operation.READ) should be (Set())
        relation.provides(Operation.WRITE) should be (Set())
        relation.requires(Operation.WRITE) should be (Set())
        relation.identifier should be (RelationIdentifier("project/fake"))
        relation.columns should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType),
            Field("some_col", StringType),
            Field("other_col", StringType),
            Field("last_col", StringType)
        ))
        relation.fields should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType),
            Field("some_col", StringType),
            Field("other_col", StringType),
            Field("last_col", StringType)
        ))
        relation.records should be (Seq(
            ArrayRecord("a","12","3"),
            ArrayRecord("cat","","7"),
            ArrayRecord("dog",null,"8")
        ))
    }

    it should "work with specified records and schema" in {
        val relationTemplate = mock[Prototype[Relation]]

        val project = Project(
            "my_project",
            relations = Map(
                "const" -> relationTemplate
            )
        )
        val schema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val valuesRelation = ValuesRelation(
            Relation.Properties(context, "const"),
            _schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = schema.fields
            )),
            records = Seq(
                ArrayRecord("lala","12"),
                ArrayRecord("lolo","13"),
                ArrayRecord("",null)
            )
        )

        (relationTemplate.instantiate _).expects(context, None).returns(valuesRelation)
        val relation = context.getRelation(RelationIdentifier("const"))

        relation.provides(Operation.CREATE) should be (Set())
        relation.requires(Operation.CREATE) should be (Set())
        relation.provides(Operation.READ) should be (Set())
        relation.requires(Operation.READ) should be (Set())
        relation.provides(Operation.WRITE) should be (Set())
        relation.requires(Operation.WRITE) should be (Set())
        relation.partitions should be (Seq())
        relation.fields should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))
        relation.describe(executor) should be (StructType(schema.fields))

        val df = relation.read(executor)
        df.schema should be (schema.sparkType)
        df.collect() should be (Seq(
            Row("lala", 12),
            Row("lolo", 13),
            Row(null,null)
        ))
    }

    it should "work with specified records and columns" in {
        val relationTemplate = mock[Prototype[Relation]]

        val project = Project(
            "my_project",
            relations = Map(
                "const" -> relationTemplate
            )
        )
        val schema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val mockRelation = ValuesRelation(
            Relation.Properties(context, "const"),
            columns = schema.fields,
            records = Seq(
                ArrayRecord("lala","12"),
                ArrayRecord("lolo","13"),
                ArrayRecord("",null)
            )
        )

        (relationTemplate.instantiate _).expects(context, None).returns(mockRelation)
        val relation = context.getRelation(RelationIdentifier("const"))

        relation.provides(Operation.CREATE) should be (Set())
        relation.requires(Operation.CREATE) should be (Set())
        relation.provides(Operation.READ) should be (Set())
        relation.requires(Operation.READ) should be (Set())
        relation.provides(Operation.WRITE) should be (Set())
        relation.requires(Operation.WRITE) should be (Set())
        relation.partitions should be (Seq())
        relation.fields should be (Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))
        relation.describe(executor) should be (StructType(schema.fields))

        val df = relation.read(executor)
        df.schema should be (schema.sparkType)
        df.collect() should be (Seq(
            Row("lala", 12),
            Row("lolo", 13),
            Row(null,null)
        ))
    }

    it should "support some lifecycle methods" in {
        val relationTemplate = mock[Prototype[Relation]]

        val project = Project(
            "my_project",
            relations = Map(
                "const" -> relationTemplate
            )
        )
        val schema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val mockRelation = ValuesRelation(
            Relation.Properties(context, "const"),
            columns = schema.fields
        )

        (relationTemplate.instantiate _).expects(context, None).returns(mockRelation)
        val relation = context.getRelation(RelationIdentifier("const"))

        relation.describe(executor) should be (StructType(schema.fields))

        relation.exists(executor) should be (Yes)
        relation.loaded(executor) should be (Yes)

        // 1. Create
        relation.create(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor) should be (Yes)

        // 2. Migrate
        relation.migrate(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor) should be (Yes)

        // 3. Write
        an[NotImplementedError] should be thrownBy(relation.write(executor, null))

        // 5. Truncate
        an[NotImplementedError] should be thrownBy(relation.truncate(executor))

        // 6. Destroy
        relation.destroy(executor)
        relation.exists(executor) should be (Yes)
        relation.loaded(executor) should be (Yes)
    }
}
