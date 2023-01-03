/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.spark.testing.LocalSparkSession

case class Record(c1:String, c2:String)


class ProvidedRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The ProvidedRelation" should "work" in {
        val spec =
            """
              |relations:
              |  dummy:
              |    kind: provided
              |    table: my_table
              |mappings:
              |  dummy:
              |    kind: relation
              |    relation: dummy
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.relations.keys should contain("dummy")
        project.mappings.keys should contain("dummy")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        executor.spark.emptyDataFrame.createOrReplaceTempView("my_table")

        val mapping = context.getMapping(MappingIdentifier("dummy"))
        mapping should not be null

        val df = executor.instantiate(mapping, "main")
        df.count should be (0)

        session.shutdown()
    }

}
