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

package com.dimajix.flowman.execution

import org.apache.spark.sql.DataFrame
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.RootExecutionTest.RangeMappingPrototype
import com.dimajix.flowman.execution.RootExecutionTest.TestMappingPrototype
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


object RootExecutionTest {
    case class TestMapping(
        instanceProperties: Mapping.Properties,
        inputs:Set[MappingOutputIdentifier]
    ) extends BaseMapping {
        override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
            Map("main" -> input.values.head)
        }
    }

    case class RangeMapping(
        instanceProperties: Mapping.Properties
    ) extends BaseMapping {
        override def inputs: Set[MappingOutputIdentifier] = Set.empty

        override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = {
            val spark = execution.spark
            Map("main" -> spark.range(0,1000).toDF())
        }
    }

    case class TestMappingPrototype(name:String, inputs:Seq[String]) extends Prototype[Mapping] {
        override def instantiate(context: Context): Mapping = {
            TestMapping(
                Mapping.Properties(context, name),
                inputs.map(i => MappingOutputIdentifier(i)).toSet
            )
        }
    }
    case class RangeMappingPrototype(name:String) extends Prototype[Mapping] {
        override def instantiate(context: Context): Mapping = {
            RangeMapping(
                Mapping.Properties(context, name)
            )
        }
    }
}
class RootExecutionTest extends AnyFlatSpec with MockFactory with Matchers with LocalSparkSession {
    "The RootExecution" should "describe mappings without parallelism" in {
        val module = Module(
            mappings = Map(
                "m0" -> RangeMappingPrototype("m0"),
                "m1" -> RangeMappingPrototype("m1"),
                "m2" -> TestMappingPrototype("m2", Seq("m1", "m0")),
                "m3" -> TestMappingPrototype("m3", Seq("m1", "m0")),
                "m4" -> TestMappingPrototype("m4", Seq("m0", "m3", "m2")),
                "m5" -> TestMappingPrototype("m5", Seq("m1", "m2")),
                "m6" -> TestMappingPrototype("m6", Seq("m4", "m1", "m2")),
                "m7" -> TestMappingPrototype("m7", Seq("m6", "m0", "m3")),
                "m8" -> TestMappingPrototype("m8", Seq("m5", "m4", "m3", "m2")),
                "m9" -> TestMappingPrototype("m9", Seq("m8", "m7", "m6"))
            )
        )
        val project = module.toProject("project")
        val session = Session.builder()
            .withSparkSession(spark)
            .withConfig(FlowmanConf.EXECUTION_MAPPING_PARALLELISM.key, "1")
            .withProject(project)
            .build()

        val execution = session.execution
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("m4"))

        val schema = execution.describe(mapping, "main")
        schema should be (StructType(Seq(
            Field("id", LongType, false)
        )))
    }

    it should "describe mappings in parallel" in {
        val module = Module(
            mappings = Map(
                "m0" -> RangeMappingPrototype("m0"),
                "m1" -> RangeMappingPrototype("m1"),
                "m2" -> TestMappingPrototype("m2", Seq("m1", "m0")),
                "m3" -> TestMappingPrototype("m3", Seq("m1", "m0")),
                "m4" -> TestMappingPrototype("m4", Seq("m0", "m3", "m2")),
                "m5" -> TestMappingPrototype("m5", Seq("m1", "m2")),
                "m6" -> TestMappingPrototype("m6", Seq("m4", "m1", "m2")),
                "m7" -> TestMappingPrototype("m7", Seq("m6", "m0", "m3")),
                "m8" -> TestMappingPrototype("m8", Seq("m5", "m4", "m3", "m2")),
                "m9" -> TestMappingPrototype("m9", Seq("m0", "m8", "m7", "m6"))
            )
        )
        val project = module.toProject("project")
        val session = Session.builder()
            .withSparkSession(spark)
            .withConfig(FlowmanConf.EXECUTION_MAPPING_PARALLELISM.key, "4")
            .withProject(project)
            .build()

        val execution = session.execution
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("m4"))

        val schema = execution.describe(mapping, "main")
        schema should be (StructType(Seq(
            Field("id", LongType, false)
        )))
    }
}
