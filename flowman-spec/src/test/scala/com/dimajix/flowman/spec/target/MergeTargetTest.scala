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

package com.dimajix.flowman.spec.target

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.expr
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.DeleteClause
import com.dimajix.flowman.execution.InsertClause
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.UpdateClause
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.Selector
import com.dimajix.flowman.model.IdentifierRelationReference
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.mapping.ProvidedMapping
import com.dimajix.flowman.spec.relation.EmptyRelation
import com.dimajix.spark.testing.LocalSparkSession


class MergeTargetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The MergeTarget" should "be parseable" in {
        val spec =
            """
              |kind: merge
              |relation: some_relation
              |mapping: some_mapping
              |mergeKey:
              | - usaf
              | - wban
              |clauses:
              | - action: update
              |   condition: "update.type = 'UPDATE' AND current.version = 12"
              |   columns:
              |     tgt_x: "some_expr(mapping.y)"
              | - action: insert
              |   condition: "update.type = 'INSERT'"
              | - action: insert
              |   condition: "update.type = 'INSERT2'"
              |   columns:
              |     tgt_x: "some_expr(mapping.y)"
              | - action: delete
              |   condition: "update.type = 'DELETE'"
              |""".stripMargin

        val targetSpec = ObjectMapper.parse[TargetSpec](spec)
        val mergeTargetSpec = targetSpec.asInstanceOf[MergeTargetSpec]

        val session = Session.builder().disableSpark().build()
        val context = session.context
        val mergeTarget = mergeTargetSpec.instantiate(context)

        mergeTarget.relation should be (IdentifierRelationReference(context, "some_relation"))
        mergeTarget.mapping should be (MappingOutputIdentifier("some_mapping"))
        mergeTarget.key should be (Seq("usaf", "wban"))
        mergeTarget.clauses should be (Seq(
            UpdateClause(
                Some(expr("update.type = 'UPDATE' AND current.version = 12")),
                Map("tgt_x" -> expr("some_expr(mapping.y)"))
            ),
            InsertClause(
                Some(expr("update.type = 'INSERT'")),
                Map()
            ),
            InsertClause(
                Some(expr("update.type = 'INSERT2'")),
                Map("tgt_x" -> expr("some_expr(mapping.y)"))
            ),
            DeleteClause(
                Some(expr("update.type = 'DELETE'"))
            )
        ))
    }

    it should "provide correct dependencies" in {
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
               |    kind: merge
               |    mapping: some_table
               |    relation: some_relation
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val target = context.getTarget(TargetIdentifier("out"))
        target.kind should be ("merge")
        target.phases should be (Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))

        target.requires(Phase.CREATE) should be (Set())
        target.requires(Phase.BUILD) should be (Set())
        target.requires(Phase.VERIFY) should be (Set())
        target.requires(Phase.TRUNCATE) should be (Set())
        target.requires(Phase.DESTROY) should be (Set())

        target.provides(Phase.CREATE) should be (Set(ResourceIdentifier.ofFile(new Path(new File("test/data/data_1.csv").getAbsoluteFile.toURI))))
        target.provides(Phase.BUILD) should be (Set(ResourceIdentifier.ofFile(new Path(new File("test/data/data_1.csv").getAbsoluteFile.toURI))))
        target.provides(Phase.VERIFY) should be (Set())
        target.provides(Phase.TRUNCATE) should be (Set())
        target.provides(Phase.DESTROY) should be (Set(ResourceIdentifier.ofFile(new Path(new File("test/data/data_1.csv").getAbsoluteFile.toURI))))
    }

    it should "count the number of records" in {
        val spark = this.spark
        import spark.implicits._

        val data = Seq(("v1", 12), ("v2", 23)).toDF()
        data.createOrReplaceTempView("some_table")

        val relationGen = Prototype.of((context:Context) => EmptyRelation(
            Relation.Properties(context)
        ).asInstanceOf[Relation])
        val mappingGen = Prototype.of((context:Context) => ProvidedMapping(
            Mapping.Properties(context),
            "some_table"
        ).asInstanceOf[Mapping])
        val targetGen = Prototype.of((context:Context) => MergeTarget(
            context,
            RelationIdentifier("relation"),
            MappingOutputIdentifier("mapping"),
            Seq(),
            Seq()
        ).asInstanceOf[Target])
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

        val metric = executor.metricSystem
            .findMetric(Selector("target_records", target.metadata.asMap))
            .head
            .asInstanceOf[GaugeMetric]

        metric.value should be (2)
        metric.labels should be (target.metadata.asMap + ("phase" -> Phase.BUILD.upper))

        target.execute(executor, Phase.BUILD)
        metric.value should be (4)
    }
}
