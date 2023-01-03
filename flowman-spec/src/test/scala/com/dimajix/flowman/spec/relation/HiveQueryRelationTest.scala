/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

import com.google.common.io.Resources
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


class HiveQueryRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "A HiveQueryRelation" should "be parseable" in {
        val spec =
            """
              |kind: hiveQuery
              |sql: |
              |  SELECT * FROM some_table
              |""".stripMargin
        val relationSpec = ObjectMapper.parse[RelationSpec](spec)
        relationSpec shouldBe(an[HiveQueryRelationSpec])
    }

    it should "work with an external file" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: hiveTable
              |    database: default
              |    table: t0
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: str_col
              |          type: string
              |        - name: int_col
              |          type: integer
              |        - name: t0_exclusive_col
              |          type: long
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        val basedir = new Path(Resources.getResource(".").toURI)

        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.getContext(project)

        val relation = HiveQueryRelation(
            Relation.Properties(context),
            file = Some(context.fs.file(new Path(basedir, "project/relation/some-view.sql")))
        )

        relation.provides(Operation.CREATE) should be(Set.empty)
        relation.requires(Operation.CREATE) should be(Set(
            ResourceIdentifier.ofHiveTable("t0", Some("default"))
        ))
        relation.provides(Operation.WRITE) should be(Set.empty)
        relation.requires(Operation.WRITE) should be(Set.empty)
        relation.provides(Operation.READ) should be(Set.empty)
        relation.requires(Operation.READ) should be(Set(
            ResourceIdentifier.ofHiveTable("t0", Some("default")),
            ResourceIdentifier.ofHivePartition("t0", Some("default"), Map())
        ))

        // == Create =================================================================================================
        context.getRelation(RelationIdentifier("t0")).create(execution)

        relation.exists(execution) should be(Yes)
        relation.loaded(execution, Map()) should be(Yes)
        relation.conforms(execution) should be(Yes)
        an[UnsupportedOperationException] should be thrownBy (relation.create(execution))
        an[UnsupportedOperationException] should be thrownBy (relation.migrate(execution))
        an[UnsupportedOperationException] should be thrownBy (relation.destroy(execution))

        // == Read ===================================================================================================
        relation.describe(execution) should be (ftypes.StructType(Seq(
            Field("str_col", ftypes.StringType),
            Field("int_col", ftypes.IntegerType),
            Field("t0_exclusive_col", ftypes.LongType)
        )))

        // == Destroy ================================================================================================
        context.getRelation(RelationIdentifier("t0")).destroy(execution)

        session.shutdown()
    }
}
