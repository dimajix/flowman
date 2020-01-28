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

import org.apache.hadoop.fs.Path
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.TargetIdentifier


class RelationTargetTest extends FlatSpec with Matchers {
    "The RelationTarget" should "work" in {
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
               |    kind: relation
               |    mapping: some_table
               |    relation: some_relation
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().build()
        val context = session.getContext(project)

        val target = context.getTarget(TargetIdentifier("out"))
        target.kind should be ("relation")

        target.requires(Phase.CREATE) should be (Set())
        target.requires(Phase.BUILD) should be (Set())
        target.requires(Phase.VERIFY) should be (Set())
        target.requires(Phase.TRUNCATE) should be (Set())
        target.requires(Phase.DESTROY) should be (Set())

        target.provides(Phase.CREATE) should be (Set(ResourceIdentifier.ofFile(new Path("test/data/data_1.csv"))))
        target.provides(Phase.BUILD) should be (Set(ResourceIdentifier.ofFile(new Path("test/data/data_1.csv"))))
        target.provides(Phase.VERIFY) should be (Set())
        target.provides(Phase.TRUNCATE) should be (Set())
        target.provides(Phase.DESTROY) should be (Set(ResourceIdentifier.ofFile(new Path("test/data/data_1.csv"))))
    }
}
