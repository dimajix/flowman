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

package com.dimajix.flowman.spec.target

import java.io.File
import java.nio.file.Paths
import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.spark.testing.LocalSparkSession


class StreamTargetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The StreamTarget" should "provide correct dependencies" in {
        val spec =
            s"""
               |mappings:
               |  some_table:
               |    kind: provided
               |    table: some_table
               |
               |relations:
               |  some_relation:
               |    kind: file
               |    location: test/data/data_1.csv
               |    format: csv
               |
               |targets:
               |  out:
               |    kind: stream
               |    mapping: some_table
               |    relation: some_relation
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val target = context.getTarget(TargetIdentifier("out"))
        target.kind should be ("stream")

        target.requires(Phase.CREATE) should be (Set())
        target.requires(Phase.BUILD) should be (Set())
        target.requires(Phase.VERIFY) should be (Set())
        target.requires(Phase.TRUNCATE) should be (Set())
        target.requires(Phase.DESTROY) should be (Set())

        target.provides(Phase.CREATE) should be (Set(ResourceIdentifier.ofFile(new Path( new File("test/data/data_1.csv").getAbsoluteFile.toURI))))
        target.provides(Phase.BUILD) should be (Set())
        target.provides(Phase.VERIFY) should be (Set())
        target.provides(Phase.TRUNCATE) should be (Set())
        target.provides(Phase.DESTROY) should be (Set(ResourceIdentifier.ofFile(new Path(new File("test/data/data_1.csv").getAbsoluteFile.toURI))))
    }

    it should "support the whole lifecycle" in {
        val inputPath = Paths.get(tempDir.toString, "test_" + UUID.randomUUID().toString)
        val outputPath = Paths.get(tempDir.toString, "test_" + UUID.randomUUID().toString)
        val spec =
            s"""
               |mappings:
               |  input:
               |    kind: readStream
               |    relation: input
               |
               |relations:
               |  input:
               |    kind: file
               |    location: ${inputPath.toUri}
               |    format: csv
               |    schema:
               |      kind: embedded
               |      fields:
               |        - name: int_col
               |          type: integer
               |        - name: dbl_col
               |          type: double
               |        - name: str_col
               |          type: string
               |  output:
               |    kind: file
               |    location: ${outputPath.toUri}
               |    format: csv
               |    schema:
               |      kind: embedded
               |      fields:
               |        - name: int_col
               |          type: integer
               |        - name: dbl_col
               |          type: double
               |        - name: str_col
               |          type: string
               |
               |targets:
               |  out:
               |    kind: stream
               |    mapping: input
               |    relation: output
               |    mode: append
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .build()
        val execution = session.execution
        val context = session.getContext(project)

        val input = context.getRelation(RelationIdentifier("input"))
        val output = context.getRelation(RelationIdentifier("output"))
        val target = context.getTarget(TargetIdentifier("out"))

        // == Create =================================================================================================
        input.exists(execution) should be (No)
        input.loaded(execution) should be (No)
        input.create(execution)
        input.exists(execution) should be (Yes)
        input.loaded(execution) should be (No)

        // == Write ==================================================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row(null, null, "1"),
            Row(234, 123.0, "1"),
            Row(2345, 1234.0, "1"),
            Row(23456, 12345.0, "2")
        ))
        val df = spark.createDataFrame(rdd, StructType(input.fields.map(_.catalogField)))
        input.write(execution, df)
        input.exists(execution) should be (Yes)
        input.loaded(execution) should be (Yes)

        // == Create =================================================================================================
        output.exists(execution) should be (No)
        output.loaded(execution) should be (No)
        target.dirty(execution, Phase.CREATE) should be (Yes)
        target.execute(execution, Phase.CREATE)
        output.exists(execution) should be (Yes)
        output.loaded(execution) should be (No)
        target.dirty(execution, Phase.CREATE) should be (Unknown)
        output.read(execution).count() should be (0)

        // == Build ==================================================================================================
        target.dirty(execution, Phase.BUILD) should be (Yes)
        target.execute(execution, Phase.BUILD)
        output.exists(execution) should be (Yes)
        output.loaded(execution) should be (Yes)
        target.dirty(execution, Phase.BUILD) should be (Yes)

        Thread.sleep(2000)
        execution.operations.stop()
        execution.operations.awaitTermination()
        output.read(execution).count() should be (4)

        // == Verify =================================================================================================

        // == Truncate ===============================================================================================
        target.dirty(execution, Phase.TRUNCATE) should be (Yes)
        target.execute(execution, Phase.TRUNCATE)
        output.exists(execution) should be (Yes)
        output.loaded(execution) should be (No)
        target.dirty(execution, Phase.TRUNCATE) should be (No)
        output.read(execution).count() should be (0)

        // == Destroy ================================================================================================
        target.dirty(execution, Phase.DESTROY) should be (Yes)
        target.execute(execution, Phase.DESTROY)
        output.exists(execution) should be (No)
        output.loaded(execution) should be (No)
        target.dirty(execution, Phase.DESTROY) should be (No)
    }

    it should "support trigger-once" in {
        val inputPath = Paths.get(tempDir.toString, "test_" + UUID.randomUUID().toString)
        val outputPath = Paths.get(tempDir.toString, "test_" + UUID.randomUUID().toString)
        val spec =
            s"""
               |mappings:
               |  input:
               |    kind: readStream
               |    relation: input
               |
               |relations:
               |  input:
               |    kind: file
               |    location: ${inputPath.toUri}
               |    format: csv
               |    schema:
               |      kind: embedded
               |      fields:
               |        - name: int_col
               |          type: integer
               |        - name: dbl_col
               |          type: double
               |        - name: str_col
               |          type: string
               |  output:
               |    kind: file
               |    location: ${outputPath.toUri}
               |    format: csv
               |    schema:
               |      kind: embedded
               |      fields:
               |        - name: int_col
               |          type: integer
               |        - name: dbl_col
               |          type: double
               |        - name: str_col
               |          type: string
               |
               |targets:
               |  out:
               |    kind: stream
               |    mapping: input
               |    relation: output
               |    mode: append
               |    trigger: once
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .build()
        val execution = session.execution
        val context = session.getContext(project)

        val input = context.getRelation(RelationIdentifier("input"))
        val output = context.getRelation(RelationIdentifier("output"))
        val target = context.getTarget(TargetIdentifier("out"))

        // == Create =================================================================================================
        input.create(execution)

        // == Write ==================================================================================================
        val rdd = spark.sparkContext.parallelize(Seq(
            Row(null, null, "1"),
            Row(234, 123.0, "1"),
            Row(2345, 1234.0, "1"),
            Row(23456, 12345.0, "2")
        ))
        val df = spark.createDataFrame(rdd, StructType(input.fields.map(_.catalogField)))
        input.write(execution, df)

        // == Create =================================================================================================
        target.execute(execution, Phase.CREATE)
        output.read(execution).count() should be (0)

        // == Build ==================================================================================================
        target.execute(execution, Phase.BUILD)
        output.exists(execution) should be (Yes)
        output.loaded(execution) should be (Yes)
        target.dirty(execution, Phase.BUILD) should be (Yes)
        output.read(execution).count() should be (4)

        // == Verify =================================================================================================

        // == Truncate ===============================================================================================
        target.execute(execution, Phase.TRUNCATE)
        output.read(execution).count() should be (0)

        // == Destroy ================================================================================================
        target.execute(execution, Phase.DESTROY)
    }
}
