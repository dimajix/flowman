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

package com.dimajix.flowman.execution

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.MappingUtilsTest.DummyMappingSpec
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Prototype


object MappingUtilsTest {
    case class DummyMapping(
        override val context: Context,
        override val name: String,
        override val inputs: Set[MappingOutputIdentifier],
        override val requires: Set[ResourceIdentifier]
    ) extends BaseMapping {
        protected override def instanceProperties: Mapping.Properties = Mapping.Properties(context, name)
        override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = ???
    }

    case class DummyMappingSpec(
        name: String,
        inputs: Seq[MappingOutputIdentifier],
        requires: Set[ResourceIdentifier]
    ) extends Prototype[Mapping] {
        override def instantiate(context: Context): Mapping = DummyMapping(context, name, inputs.toSet, requires)
    }
}

class MappingUtilsTest extends AnyFlatSpec with Matchers {
    "The MappingUtils" should "glob all requirements of a mapping" in {
        val project = Project(
            "test",
            mappings = Map(
                "m1" -> DummyMappingSpec(
                    "m1",
                    Seq(),
                    Set(ResourceIdentifier.ofFile(new Path("file1")))
                ),
                "m2" -> DummyMappingSpec(
                    "m2",
                    Seq(),
                    Set(ResourceIdentifier.ofFile(new Path("file1")), ResourceIdentifier.ofFile(new Path("file2")))
                ),
                "m3" -> DummyMappingSpec(
                    "m3",
                    Seq(MappingOutputIdentifier("m1")),
                    Set(ResourceIdentifier.ofFile(new Path("file3")))
                ),
                "m4" -> DummyMappingSpec(
                    "m4",
                    Seq(MappingOutputIdentifier("m1"), MappingOutputIdentifier("m2"), MappingOutputIdentifier("m3")),
                    Set()
                )
            )
        )

        val session = Session.builder()
            .disableSpark()
            .build()

        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("m4"))

        val expected = Set(
            ResourceIdentifier.ofFile(new Path("file1")),
            ResourceIdentifier.ofFile(new Path("file2")),
            ResourceIdentifier.ofFile(new Path("file3"))
        )
        MappingUtils.requires(mapping) should be (expected)
        MappingUtils.requires(context, MappingIdentifier("m4")) should be (expected)
    }
}
