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

package com.dimajix.flowman.spec.model

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module


class FileRelationTest extends FlatSpec with Matchers with LocalSparkSession {
    "The FileRelation" should "be parseable" in {
        val spec =
            """
              |relations:
              |  t0:
              |    kind: file
              |    format: csv
              |    location: test/data/data_1.csv
              |    schema:
              |      kind: embedded
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

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.getExecutor(project)
        val relation = project.relations("t0")
        val df = relation.read(executor, null)
        df.schema should be (StructType(
            StructField("f1", StringType) ::
            StructField("f2", StringType) ::
            StructField("f3", StringType) ::
            Nil
        ))
        df.collect()
    }
}
