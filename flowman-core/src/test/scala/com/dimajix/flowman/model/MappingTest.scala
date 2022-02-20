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

package com.dimajix.flowman.model

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions => f}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.NoSuchMappingOutputException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.graph.GraphBuilder
import com.dimajix.flowman.graph.InputMapping
import com.dimajix.flowman.model.MappingTest.DummyMapping
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


object MappingTest {
    class DummyMapping(props:Mapping.Properties, ins:Set[MappingOutputIdentifier]) extends BaseMapping {
        protected override def instanceProperties: Mapping.Properties = props

        override def inputs: Set[MappingOutputIdentifier] = ins

        override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
            val df = input.head._2.groupBy("id").agg(f.sum("val"))
            Map("main" -> df)
        }
    }
}

class MappingTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "Mappings" should "work" in {
        val project = Project(
            name = "project"
        )
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping = new DummyMapping(
            Mapping.Properties(context, "m1"),
            Set()
        )

        mapping.metadata should be (Metadata(
            None,
            Some("project"),
            "m1",
            None,
            "mapping",
            "",
            Map()
        ))
    }

    "Mapping.output" should "return a MappingOutputIdentifier with a project" in {
        val project = Project(
            name = "project"
        )
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping = new DummyMapping(
            Mapping.Properties(context, "m1"),
            Set()
        )
        mapping.output("main") should be (MappingOutputIdentifier("project/m1:main"))
        an[NoSuchMappingOutputException] should be thrownBy(mapping.output("no_such_output"))
    }

    it should "return a MappingOutputIdentifier without a project" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context

        val mapping = new DummyMapping(
            Mapping.Properties(context, "m1"),
            Set()
        )
        mapping.output("main") should be (MappingOutputIdentifier("m1:main"))
        an[NoSuchMappingOutputException] should be thrownBy(mapping.output("no_such_output"))
    }

    "Mapping.describe default implementation" should "return meaningful results" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val mapping = new DummyMapping(
            Mapping.Properties(context, "m1"),
            Set(MappingOutputIdentifier("input:main"))
        )

        val inputSchema = StructType(Seq(
            Field("id", StringType),
            Field("val", IntegerType),
            Field("comment", StringType)
        ))
        val result = mapping.describe(execution, Map(MappingOutputIdentifier("input:main") -> inputSchema))

        result("main") should be (StructType(Seq(
            Field("id", StringType),
            Field("sum(val)", LongType)
        )))
    }

    "Mapping.link default implementation" should "work" in {
        val mappingTemplate1 = mock[Prototype[Mapping]]
        val mappingTemplate2 = mock[Prototype[Mapping]]

        val project = Project(
            name = "project",
            mappings = Map(
                "m1" -> mappingTemplate1,
                "m2" -> mappingTemplate2
            )
        )
        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)

        val mapping1 = new DummyMapping(
            Mapping.Properties(context, "m1"),
            Set(MappingOutputIdentifier("m2"))
        )
        val mapping2 = new DummyMapping(
            Mapping.Properties(context, "m2"),
            Set()
        )
        //(mappingTemplate1.instantiate _).expects(context).returns(mapping1)
        (mappingTemplate2.instantiate _).expects(context).returns(mapping2)

        val graphBuilder = new GraphBuilder(context, Phase.BUILD)
        val ref1 = graphBuilder.refMapping(mapping1)
        val ref2 = graphBuilder.refMapping(mapping2)
        val out11 = ref1.outputs.head
        val out21 = ref2.outputs.head

        ref1.mapping should be (mapping1)
        ref1.incoming should be (Seq(
            InputMapping(out21, ref1)
        ))
        ref1.outgoing should be (Seq())

        ref2.mapping should be (mapping2)
        ref2.incoming should be (Seq())
        ref2.outgoing should be (Seq())
        ref2.outputs.head.outgoing should be (Seq(
            InputMapping(out21, ref1)
        ))
    }
}
