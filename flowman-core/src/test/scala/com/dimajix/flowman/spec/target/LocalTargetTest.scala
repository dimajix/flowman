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

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.testing.LocalSparkSession


class LocalTargetTest extends FlatSpec with Matchers with LocalSparkSession {
    "A LocalTarget" should "be buildable" in {
        val spark = this.spark
        val outputPath = Paths.get(tempDir.toString, "local-target", "data.csv")

        val spec =
            s"""
               |targets:
               |  out:
               |    kind: local
               |    input: some_table
               |    filename: $outputPath
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val executor = session.executor
        val context = session.getContext(project)

        import spark.implicits._
        val data = Seq(("v1", 12), ("v2", 23)).toDF()
        val output = context.getTarget(TargetIdentifier("out"))

        outputPath.toFile.exists() should be (false)
        output.build(executor, Map(MappingOutputIdentifier("some_table") -> data))
        outputPath.toFile.exists() should be (true)

        outputPath.toFile.exists() should be (true)
        output.clean(executor)
        outputPath.toFile.exists() should be (false)
    }

}
