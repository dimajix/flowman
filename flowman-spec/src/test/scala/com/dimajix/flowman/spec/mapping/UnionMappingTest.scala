/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.mapping

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.graph.Graph
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class UnionMappingTest  extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The Union Mapping" should "be readable from YML" in {
        val spec =
            """
              |mappings:
              |  union:
              |    kind: union
              |    inputs:
              |      - src_1
              |      - src_2
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")

        project.mappings.size should be (1)
        project.mappings.contains("union") should be (true)
    }

    it should "work" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val mapping = UnionMapping(
            Mapping.Properties(context, "union", "union"),
            Seq(MappingOutputIdentifier("m1"), MappingOutputIdentifier("m2"))
        )
        val df1 = spark.createDataFrame(Seq((1,2), (2,3)))
        val df2 = spark.createDataFrame(Seq((1,2), (3,4)))

        val results = mapping.execute(execution, Map(MappingOutputIdentifier("m1") -> df1, MappingOutputIdentifier("m2") -> df2))
        results.size should be (1)

        val result = results("main").orderBy("_1", "_2")
        val rows = result.as[(Int,Int)]collect()
        rows should be (Seq(
            (1,2),
            (1,2),
            (2,3),
            (3,4)
        ))
    }

    it should "support distinct" in {
        val spark = this.spark
        import spark.implicits._

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val mapping = UnionMapping(
            Mapping.Properties(context, "union", "union"),
            Seq(MappingOutputIdentifier("m1"), MappingOutputIdentifier("m2")),
            distinct = true
        )
        val df1 = spark.createDataFrame(Seq((1,2), (2,3)))
        val df2 = spark.createDataFrame(Seq((1,2), (3,4)))

        val results = mapping.execute(execution, Map(MappingOutputIdentifier("m1") -> df1, MappingOutputIdentifier("m2") -> df2))
        results.size should be (1)

        val result = results("main").orderBy("_1", "_2")
        val rows = result.as[(Int,Int)]collect()
        rows should be (Seq(
            (1,2),
            (2,3),
            (3,4)
        ))
    }

    it should "support describe" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val mapping = UnionMapping(
            Mapping.Properties(context, "union", "union"),
            Seq(MappingOutputIdentifier("m1"), MappingOutputIdentifier("m2"))
        )
        val sc1 = StructType(Seq(
            Field("_1", IntegerType),
            Field("_2", StringType)
        ))
        val sc2 = StructType(Seq(
            Field("_2", StringType),
            Field("_1", IntegerType)
        ))

        val results = mapping.describe(execution, Map(MappingOutputIdentifier("m1") -> sc1, MappingOutputIdentifier("m2") -> sc2))
        results.size should be (1)

        val result = results("main")
        result should be(StructType(Seq(
            Field("_1", IntegerType),
            Field("_2", StringType)
        )))
    }

    it should "support linking" in {
        val unionGen = mock[Prototype[Mapping]]
        val m1Gen = mock[Prototype[Mapping]]
        val m2Gen = mock[Prototype[Mapping]]
        val project = Project(
            name = "project",
            mappings = Map(
                "union" -> unionGen,
                "m1" -> m1Gen,
                "m2" -> m2Gen,
            )
        )
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)

        val union = UnionMapping(
            Mapping.Properties(context, "union", "union"),
            Seq(MappingOutputIdentifier("m1"), MappingOutputIdentifier("m2"))
        )
        val m1 = ValuesMapping(
            Mapping.Properties(context, "m1", "m1"),
            columns = Seq(
                Field("col1", StringType),
                Field("col2", IntegerType)
            )
        )
        val m2 = ValuesMapping(
            Mapping.Properties(context, "m2", "m2"),
            columns = Seq(
                Field("col1", StringType),
                Field("col3", DoubleType)
            )
        )

        (unionGen.instantiate _).expects(context).returns(union)
        (m1Gen.instantiate _).expects(context).returns(m1)
        (m2Gen.instantiate _).expects(context).returns(m2)

        val graph = Graph.ofProject(context, project, Phase.BUILD)
        val mapNode = graph.mapping(union)
        val mapCols = mapNode.outputs.head.fields
        mapCols.map(_.name).sorted should be (Seq("col1", "col2", "col3"))

        val col1 = mapCols.find(_.name == "col1").get
        col1.incoming.map(_.input.fqName).sorted should be (Seq("[m1:main].col1","[m2:main].col1"))
        val col2 = mapCols.find(_.name == "col2").get
        col2.incoming.map(_.input.fqName) should be (Seq("[m1:main].col2"))
        val col3 = mapCols.find(_.name == "col3").get
        col3.incoming.map(_.input.fqName) should be (Seq("[m2:main].col3"))
    }
}
