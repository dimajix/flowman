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

package com.dimajix.flowman.spec.target

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.spark.testing.LocalSparkSession


class BlackholeTargetTest extends FlatSpec with Matchers with LocalSparkSession{
    "A Blackhole Target" should "be buildable" in {
        val spark = this.spark

        val spec =
            s"""
               |mappings:
               |  some_table:
               |    kind: provided
               |    table: some_table
               |
               |targets:
               |  out:
               |    kind: blackhole
               |    mapping: some_table
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val context = session.getContext(project)

        spark.emptyDataFrame.createOrReplaceTempView("some_table")

        val output = context.getTarget(TargetIdentifier("out"))
        output.execute(executor, Phase.BUILD)
        output.execute(executor, Phase.TRUNCATE)
    }
}
