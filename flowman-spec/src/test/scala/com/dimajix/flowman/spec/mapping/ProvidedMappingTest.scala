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

package com.dimajix.flowman.spec.mapping

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.spark.testing.LocalSparkSession


class ProvidedMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    "The ProvidedMapping" should "work" in {
        val spec =
            """
              |mappings:
              |  dummy:
              |    kind: provided
              |    table: my_table
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        project.mappings.keys should contain("dummy")

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        executor.spark.emptyDataFrame.createOrReplaceTempView("my_table")

        val mapping = context.getMapping(MappingIdentifier("dummy"))
        mapping should not be null

        val df = executor.instantiate(mapping, "main")
        df.count should be(0)
    }

}
