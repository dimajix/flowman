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

import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.spark.testing.LocalSparkSession


class AvroRelationTest extends FlatSpec with Matchers with LocalSparkSession {
    "An Avro Hive Table" should "be writeable" in (if (hiveSupported && spark.version >= "2.4") {
        val spark = this.spark
        import spark.implicits._

        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: table
               |    database: default
               |    table: avro_test
               |    format: avro
               |    location: "${tempDir.toURI}/avro_test1"
               |    writer: spark
               |    schema:
               |      kind: inline
               |      fields:
               |        - name: parent
               |          type:
               |            kind: struct
               |            fields:
               |              - name: pair
               |                type:
               |                  kind: struct
               |                  fields:
               |                    - name: str_col
               |                      type: string
               |                    - name: int_col
               |                      type: int
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        val jsons = Seq("""{ "parent" : { "pair" : { "str_col":"lala", "int_col":7 } } }""")
        val df = spark.read
            .schema(StructType(relation.schema.get.fields.map(_.sparkField)))
            .json(spark.createDataset(jsons))

        //df.printSchema()
        //df.show()

        relation.create(executor)
        relation.write(executor, df)
    })

    "Avro files" should "be writeable" in {
        val spark = this.spark
        import spark.implicits._

        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: file
               |    location: "${tempDir.toURI}/avro_test2"
               |    format: com.databricks.spark.avro
               |    schema:
               |      kind: inline
               |      fields:
               |        - name: parent
               |          type:
               |            kind: struct
               |            fields:
               |              - name: pair
               |                type:
               |                  kind: struct
               |                  fields:
               |                    - name: str_col
               |                      type: string
               |                    - name: int_col
               |                      type: int
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        val relation = context.getRelation(RelationIdentifier("t0"))

        val jsons = Seq("""{ "parent" : { "pair" : { "str_col":"lala", "int_col":7 } } }""")
        val df = spark.read
            .schema(StructType(relation.schema.get.fields.map(_.sparkField)))
            .json(spark.createDataset(jsons))

        df.printSchema()
        df.show()

        relation.create(executor)
        relation.write(executor, df)
    }
}
