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

import java.nio.file.Paths

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.spark.testing.LocalSparkSession


class LocalTargetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "A LocalTarget" should "be buildable" in {
        val spark = this.spark
        val outputPath = Paths.get(tempDir.toString, "local-target", "data.csv")

        val spec =
            s"""
               |mappings:
               |  some_table:
               |    kind: provided
               |    table: some_table
               |
               |targets:
               |  out:
               |    kind: local
               |    mapping: some_table
               |    filename: $outputPath
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val executor = session.execution
        val context = session.getContext(project)

        import spark.implicits._
        val data = Seq(("v1", 12), ("v2", 23)).toDF()
        data.createOrReplaceTempView("some_table")
        val output = context.getTarget(TargetIdentifier("out"))

        // == BUILD ===================================================================
        outputPath.toFile.exists() should be (false)
        output.dirty(executor, Phase.BUILD) should be (Yes)
        output.execute(executor, Phase.BUILD)
        output.dirty(executor, Phase.BUILD) should be (No)
        outputPath.toFile.exists() should be (true)

        // == VERIFY ===================================================================
        output.dirty(executor, Phase.VERIFY) should be (Yes)
        output.execute(executor, Phase.VERIFY)
        output.dirty(executor, Phase.VERIFY) should be (Yes)

        // == TRUNCATE ===================================================================
        outputPath.toFile.exists() should be (true)
        output.dirty(executor, Phase.TRUNCATE) should be (Yes)
        output.execute(executor, Phase.TRUNCATE)
        output.dirty(executor, Phase.TRUNCATE) should be (No)
        outputPath.toFile.exists() should be (false)

        // == DESTROY ===================================================================
        output.dirty(executor, Phase.DESTROY) should be (No)
        output.execute(executor, Phase.DESTROY)
        output.dirty(executor, Phase.DESTROY) should be (No)

        session.shutdown()
    }
}
