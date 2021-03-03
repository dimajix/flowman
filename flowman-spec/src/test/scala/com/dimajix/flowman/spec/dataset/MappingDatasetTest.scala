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

package com.dimajix.flowman.spec.dataset

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.BaseMapping
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.spec.dataset.MappingDatasetTest.DummyMappingSpec
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


object MappingDatasetTest {
    case class DummyMapping(
        override val context:Context,
        override val name:String,
        override val requires: Set[ResourceIdentifier]
    ) extends BaseMapping {
        protected override def instanceProperties: Mapping.Properties = Mapping.Properties(context, name)

        override def inputs: Seq[MappingOutputIdentifier] = Seq()
        override def execute(execution: Execution, input: Map[MappingOutputIdentifier, DataFrame]): Map[String, DataFrame] = Map("main" ->  execution.spark.emptyDataFrame)
        override def describe(execution: Execution, input: Map[MappingOutputIdentifier, StructType]): Map[String, StructType] = Map("main"-> new StructType())
    }

    case class DummyMappingSpec(
        name: String,
        requires: Set[ResourceIdentifier]
    ) extends Template[Mapping] {
        override def instantiate(context: Context): Mapping = DummyMapping(context, name, requires)
    }
}

class MappingDatasetTest extends FlatSpec with Matchers with LocalSparkSession {
    "The MappingDataset" should "work" in {
        val project = Project(
            name="test",
            mappings = Map("mapping" -> DummyMappingSpec(
                "mapping",
                Set(ResourceIdentifier.ofFile(new Path("file1")))
            ))
        )

        val session = Session.builder.withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.execution

        val dataset = MappingDataset(
            context,
            MappingOutputIdentifier("mapping")
        )

        dataset.provides should be (Set())
        dataset.requires should be (Set(ResourceIdentifier.ofFile(new Path("file1"))))
        dataset.exists(executor) should be (Yes)
        an[UnsupportedOperationException] should be thrownBy(dataset.clean(executor))
        dataset.read(executor, None).count() should be (0)
        an[UnsupportedOperationException] should be thrownBy(dataset.write(executor, null, OutputMode.APPEND))
        dataset.describe(executor) should be (Some(new StructType()))
    }
}
