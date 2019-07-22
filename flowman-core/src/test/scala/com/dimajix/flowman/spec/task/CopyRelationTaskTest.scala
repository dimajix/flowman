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

package com.dimajix.flowman.spec.task

import java.io.File

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.history.Status
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.testing.LocalSparkSession


class CopyRelationTaskTest extends FlatSpec with Matchers with LocalSparkSession {
    "A CopyRelationTask" should "support configuration via YML" in {
        val spec =
            """
              |kind: copyRelation
              |source: local_file
              |sourcePartitions:
              |  spc: part_value
              |target: some_hive_table
              |targetPartition:
              |  tpc: p2
              |mode: append
              |""".stripMargin
        val session = Session.builder().build()
        val context = session.context

        val taskSpec = ObjectMapper.parse[TaskSpec](spec).asInstanceOf[CopyRelationTaskSpec]
        val task = taskSpec.instantiate(context)
        task.source should be (RelationIdentifier("local_file"))
        task.sourcePartitions should be (Map("spc" -> SingleValue("part_value")))
        task.target should be (RelationIdentifier("some_hive_table"))
        task.targetPartition should be (Map("tpc" -> SingleValue("p2")))
        task.mode should be ("append")
    }

    it should "work" in {
        val spec =
            s"""
              |relations:
              |  source_relation:
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
              |  target_relation:
              |    kind: local
              |    format: csv
              |    location: $tempDir/copy-relation-output.csv
              |    schema:
              |      kind: embedded
              |      fields:
              |        - name: f1
              |          type: string
              |        - name: f2
              |          type: string
              |        - name: f3
              |          type: string
              |
              |jobs:
              |  main:
              |    tasks:
              |     - kind: copyRelation
              |       source: source_relation
              |       target: target_relation
              |       mode: overwrite
              |""".stripMargin
        val project = Module.read.string(spec).toProject("test")
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.getContext(project)

        val targetFilename = new File(tempDir, "copy-relation-output.csv")
        targetFilename.exists() should be (false)

        val job = context.getJob(JobIdentifier("main"))
        job should not be (null)
        job.execute(executor, Map()) shouldBe (Status.SUCCESS)
        targetFilename.exists() should be (true)
        targetFilename.isFile() should be (true)
    }
}
