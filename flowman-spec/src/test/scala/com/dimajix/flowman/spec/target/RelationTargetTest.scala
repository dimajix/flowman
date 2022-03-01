/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Unknown
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.Selector
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
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.spec.dataset.RelationDatasetSpec
import com.dimajix.flowman.spec.mapping.ProvidedMapping
import com.dimajix.flowman.spec.relation.NullRelation
import com.dimajix.spark.testing.LocalSparkSession


class RelationTargetTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The RelationTarget" should "support embedded relations" in {
        val spec =
            """
              |kind: relation
              |relation:
              |  kind: null
              |  name: ${target_name}
              |mapping: some_mapping
              |""".stripMargin
        val ds = ObjectMapper.parse[TargetSpec](spec)
        ds shouldBe a[RelationTargetSpec]

        val session = Session.builder().withEnvironment("target_name", "abc").disableSpark().build()
        val context = session.context

        val instance = ds.instantiate(context)
        instance shouldBe a[RelationTarget]

        val rt = instance.asInstanceOf[RelationTarget]
        rt.relation.name should be ("abc")
        rt.relation.identifier should be (RelationIdentifier("abc"))
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
               |    kind: relation
               |    mapping: some_table
               |    relation: some_relation
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val target = context.getTarget(TargetIdentifier("out"))
        target.kind should be ("relation")
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

    it should "work without a mapping" in {
        val spec =
            s"""
               |relations:
               |  some_relation:
               |    kind: file
               |    location: test/data/data_1.csv
               |    format: csv
               |
               |targets:
               |  out:
               |    kind: relation
               |    relation: some_relation
            """.stripMargin
        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val target = context.getTarget(TargetIdentifier("out"))
        target.kind should be ("relation")
        target.phases should be (Set(Phase.CREATE, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))

        target.requires(Phase.CREATE) should be (Set())
        target.requires(Phase.BUILD) should be (Set())
        target.requires(Phase.VERIFY) should be (Set())
        target.requires(Phase.TRUNCATE) should be (Set())
        target.requires(Phase.DESTROY) should be (Set())

        target.provides(Phase.CREATE) should be (Set(ResourceIdentifier.ofFile(new Path(new File("test/data/data_1.csv").getAbsoluteFile.toURI))))
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
               |    kind: read
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
               |    kind: relation
               |    mapping: input
               |    relation: output
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
        target.dirty(execution, Phase.CREATE) should be (No)
        output.read(execution).count() should be (0)

        // == Build ==================================================================================================
        target.dirty(execution, Phase.BUILD) should be (Yes)
        target.execute(execution, Phase.BUILD)
        output.exists(execution) should be (Yes)
        output.loaded(execution) should be (Yes)
        target.dirty(execution, Phase.BUILD) should be (No)
        output.read(execution).count() should be (4)

        // == Verify =================================================================================================
        target.dirty(execution, Phase.VERIFY) should be (Yes)
        target.execute(execution, Phase.VERIFY)
        output.exists(execution) should be (Yes)
        output.loaded(execution) should be (Yes)
        target.dirty(execution, Phase.VERIFY) should be (Yes)
        output.read(execution).count() should be (4)

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
            context,
            RelationIdentifier("relation"),
            MappingOutputIdentifier("mapping")
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

        val metric = executor.metricSystem
            .findMetric(Selector("target_records", target.metadata.asMap))
            .head
            .asInstanceOf[GaugeMetric]

        metric.value should be (2)
        metric.labels should be (target.metadata.asMap + ("phase" -> Phase.BUILD.upper))

        target.execute(executor, Phase.BUILD)
        metric.value should be (4)
    }

    it should "behave correctly with VerifyPolicy=EMPTY_AS_FAILURE" in {
        val relationGen = mock[Prototype[Relation]]
        val relation = mock[Relation]
        val project = Project(
            name = "test",
            relations = Map("relation" -> relationGen)
        )

        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .withConfig("flowman.default.target.verifyPolicy","empty_as_failure")
            .build()
        val executor = session.execution
        val context = session.getContext(project)

        val target = RelationTarget(
            context,
            RelationIdentifier("relation"),
            MappingOutputIdentifier("mapping")
        )
        (relationGen.instantiate _).expects(context).returns(relation)

        (relation.loaded _).expects(*,*).returns(Yes)
        target.execute(executor, Phase.VERIFY).withoutTime should be(TargetResult(target, Phase.VERIFY, Status.SUCCESS).withoutTime)

        (relation.loaded _).expects(*,*).returns(Unknown)
        target.execute(executor, Phase.VERIFY).withoutTime should be(TargetResult(target, Phase.VERIFY, Status.SUCCESS).withoutTime)

        (relation.loaded _).expects(*,*).returns(No)
        target.execute(executor, Phase.VERIFY).status should be(Status.FAILED)
    }

    it should "behave correctly with VerifyPolicy=EMPTY_AS_SUCCESS" in {
        val relationGen = mock[Prototype[Relation]]
        val relation = mock[Relation]
        val project = Project(
            name = "test",
            relations = Map("relation" -> relationGen)
        )

        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .withConfig("flowman.default.target.verifyPolicy","EMPTY_AS_SUCCESS")
            .build()
        val executor = session.execution
        val context = session.getContext(project)

        val target = RelationTarget(
            context,
            RelationIdentifier("relation"),
            MappingOutputIdentifier("mapping")
        )
        (relationGen.instantiate _).expects(context).returns(relation)

        (relation.loaded _).expects(*,*).returns(Yes)
        target.execute(executor, Phase.VERIFY).withoutTime should be(TargetResult(target, Phase.VERIFY, Status.SUCCESS).withoutTime)

        (relation.loaded _).expects(*,*).returns(Unknown)
        target.execute(executor, Phase.VERIFY).withoutTime should be(TargetResult(target, Phase.VERIFY, Status.SUCCESS).withoutTime)

        (relation.loaded _).expects(*,*).returns(No)
        (relation.exists _).expects(*).returns(Yes)
        target.execute(executor, Phase.VERIFY).withoutTime should be(TargetResult(target, Phase.VERIFY, Status.SUCCESS).withoutTime)

        (relation.loaded _).expects(*,*).returns(No)
        (relation.exists _).expects(*).returns(Unknown)
        target.execute(executor, Phase.VERIFY).status should be(Status.SUCCESS)

        (relation.loaded _).expects(*,*).returns(No)
        (relation.exists _).expects(*).returns(No)
        target.execute(executor, Phase.VERIFY).status should be(Status.FAILED)
    }

    it should "behave correctly with VerifyPolicy=EMPTY_AS_SUCCESS_WITH_ERRORS" in {
        val relationGen = mock[Prototype[Relation]]
        val relation = mock[Relation]
        val project = Project(
            name = "test",
            relations = Map("relation" -> relationGen)
        )

        val session = Session.builder()
            .withSparkSession(spark)
            .withProject(project)
            .withConfig("flowman.default.target.verifyPolicy","EMPTY_AS_SUCCESS_WITH_ERRORS")
            .build()
        val executor = session.execution
        val context = session.getContext(project)

        val target = RelationTarget(
            context,
            RelationIdentifier("relation"),
            MappingOutputIdentifier("mapping")
        )
        (relationGen.instantiate _).expects(context).returns(relation)

        (relation.loaded _).expects(*,*).returns(Yes)
        target.execute(executor, Phase.VERIFY).withoutTime should be(TargetResult(target, Phase.VERIFY, Status.SUCCESS).withoutTime)

        (relation.loaded _).expects(*,*).returns(Unknown)
        target.execute(executor, Phase.VERIFY).withoutTime should be(TargetResult(target, Phase.VERIFY, Status.SUCCESS).withoutTime)

        (relation.loaded _).expects(*,*).returns(No)
        (relation.exists _).expects(*).returns(Yes)
        target.execute(executor, Phase.VERIFY).withoutTime should be(TargetResult(target, Phase.VERIFY, Status.SUCCESS_WITH_ERRORS).withoutTime)

        (relation.loaded _).expects(*,*).returns(No)
        (relation.exists _).expects(*).returns(Unknown)
        target.execute(executor, Phase.VERIFY).status should be(Status.SUCCESS_WITH_ERRORS)

        (relation.loaded _).expects(*,*).returns(No)
        (relation.exists _).expects(*).returns(No)
        target.execute(executor, Phase.VERIFY).status should be(Status.FAILED)
    }
}
