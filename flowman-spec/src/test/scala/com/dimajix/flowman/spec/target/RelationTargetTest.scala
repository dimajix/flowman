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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.Selector
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.mapping.ProvidedMapping
import com.dimajix.flowman.spec.relation.NullRelation
import com.dimajix.spark.testing.LocalSparkSession


class RelationTargetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
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

    it should "count the number of records" in {
        val spark = this.spark
        import spark.implicits._

        val data = Seq(("v1", 12), ("v2", 23)).toDF()
        data.createOrReplaceTempView("some_table")

        val relationGen = (context:Context) => NullRelation(
            Relation.Properties(context)
        )
        val mappingGen = (context:Context) => ProvidedMapping(
            Mapping.Properties(context),
            "some_table"
        )
        val targetGen = (context:Context) => RelationTarget(
            Target.Properties(context),
            MappingOutputIdentifier("mapping"),
            RelationIdentifier("relation")
        )
        val project = Project(
            name = "test",
            targets = Map("target" -> targetGen),
            relations = Map("relation" -> relationGen),
            mappings = Map("mapping" -> mappingGen)
        )

        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .build()
        val executor = session.execution
        val context = session.getContext(project)

        val target = context.getTarget(TargetIdentifier("target"))
        target.execute(executor, Phase.BUILD)

        val metric = executor.metrics
            .findMetric(Selector(Some("target_records"), target.metadata.asMap))
            .head
            .asInstanceOf[GaugeMetric]

        metric.value should be (2)

        target.execute(executor, Phase.BUILD)
        metric.value should be (4)
    }
}
