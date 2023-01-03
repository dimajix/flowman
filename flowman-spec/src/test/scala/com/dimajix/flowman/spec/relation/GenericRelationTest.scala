/*
 * Copyright 2018 Kaya Kupferschmidt
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
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types.Field
import com.dimajix.spark.testing.LocalSparkSession


class GenericRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The GenericRelation" should "be parseable" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: generic
              |    format: csv
              |    options:
              |      path: test/data/data_1.csv
              |    schema:
              |      kind: inline
              |      fields:
              |        - name: f1
              |          type: string
              |        - name: f2
              |          type: string
              |        - name: f3
              |          type: string
              |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("t0")

        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))
        relation.kind should be ("generic")

        val fileRelation = relation.asInstanceOf[GenericRelation]
        fileRelation.format should be ("csv")
        fileRelation.options should be (Map("path" -> "test/data/data_1.csv"))

        session.shutdown()
    }

    it should "read data" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val schema = InlineSchema(
            Schema.Properties(session.context),
            fields = Seq(
                Field("f1", com.dimajix.flowman.types.StringType),
                Field("f2", com.dimajix.flowman.types.StringType),
                Field("f3", com.dimajix.flowman.types.StringType)
            )
        )
        val relation = GenericRelation(
            Relation.Properties(
                session.context
            ),
            Some(schema),
            "csv",
            Map("path" -> Resources.getResource("data/data_1.csv").toString)
        )

        // Verify schema
        relation.fields should be (schema.fields)

        // Verify read operation
        val df = relation.read(executor)
        df.schema should be (StructType(
            StructField("f1", StringType) ::
                StructField("f2", StringType) ::
                StructField("f3", StringType) ::
                Nil
        ))
        df.collect()

        session.shutdown()
    }
}
