/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import com.google.common.io.Resources
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.dataset.RelationDataset
import com.dimajix.flowman.types.SingleValue
import com.dimajix.spark.testing.LocalSparkSession


class CopyTargetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "A CopyTarget" should "support configuration via YML" in {
        val spec =
            """
              |kind: copy
              |source:
              |  kind: relation
              |  relation: local_file
              |  partition:
              |    spc: part_value
              |target:
              |  kind: relation
              |  relation: some_hive_table
              |  partition:
              |    tpc: p2
              |mode: append
              |""".stripMargin
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context

        val targetSpec = ObjectMapper.parse[TargetSpec](spec).asInstanceOf[CopyTargetSpec]
        val target = targetSpec.instantiate(context)
        target.source should be (RelationDataset(context, RelationIdentifier("local_file"), Map("spc" -> SingleValue("part_value"))))
        target.target should be (RelationDataset(context, RelationIdentifier("some_hive_table"), Map("tpc" -> SingleValue("p2"))))
        target.mode should be (OutputMode.APPEND)
    }

    it should "work" in {
        val srcFile = Resources.getResource(classOf[CopyTargetTest], "/data/data_1.csv")
        val spec =
            s"""
              |relations:
              |  source_relation:
              |    kind: file
              |    format: csv
              |    location: $srcFile
              |    schema:
              |      kind: inline
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
              |      kind: inline
              |      fields:
              |        - name: f1
              |          type: string
              |        - name: f2
              |          type: string
              |        - name: f3
              |          type: string
              |
              |targets:
              |  main:
              |    kind: copy
              |    source:
              |      kind: relation
              |      relation: source_relation
              |    target:
              |      kind: relation
              |      relation: target_relation
              |    mode: overwrite
              |""".stripMargin
        val project = Module.read.string(spec).toProject("test")
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.getContext(project)

        val targetFilename = new File(tempDir, "copy-relation-output.csv")
        targetFilename.exists() should be (false)

        val target = context.getTarget(TargetIdentifier("main"))
        target should not be (null)

        target.provides(Phase.CREATE) should be(Set())
        target.provides(Phase.BUILD) should be(Set(ResourceIdentifier.ofLocal(new File(tempDir, "copy-relation-output.csv"))))
        target.provides(Phase.VERIFY) should be(Set())
        target.provides(Phase.TRUNCATE) should be(Set())
        target.provides(Phase.DESTROY) should be(Set())

        target.requires(Phase.CREATE) should be(Set())
        target.requires(Phase.BUILD) should be(Set(ResourceIdentifier.ofFile(new Path(srcFile.toURI))))
        target.requires(Phase.VERIFY) should be(Set())
        target.requires(Phase.TRUNCATE) should be(Set())
        target.requires(Phase.DESTROY) should be(Set())

        // == BUILD ===================================================================
        target.dirty(executor, Phase.BUILD) should be (Yes)
        target.execute(executor, Phase.BUILD)
        target.dirty(executor, Phase.BUILD) should be (No)
        targetFilename.exists() should be (true)
        targetFilename.isFile() should be (true)

        // == VERIFY ===================================================================
        target.dirty(executor, Phase.VERIFY) should be (Yes)
        target.execute(executor, Phase.VERIFY)
        target.dirty(executor, Phase.VERIFY) should be (Yes)

        // == TRUNCATE ===================================================================
        target.dirty(executor, Phase.TRUNCATE) should be (Yes)
        target.execute(executor, Phase.TRUNCATE)
        target.dirty(executor, Phase.TRUNCATE) should be (No)

        // == DESTROY ===================================================================
        target.dirty(executor, Phase.DESTROY) should be (No)
        target.execute(executor, Phase.DESTROY)
        target.dirty(executor, Phase.DESTROY) should be (No)
    }
}
