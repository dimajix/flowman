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

package com.dimajix.flowman.spec.mapping

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.spark.testing.LocalSparkSession


class AliasMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "An AliasMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  my_alias:
              |    kind: alias
              |    input: some_mapping
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val mapping = project.mappings("my_alias")

        mapping shouldBe an[AliasMappingSpec]

        val session = Session.builder().disableSpark().build()
        val context = session.getContext(project)
        val instance = context.getMapping(MappingIdentifier("my_alias"))
        instance shouldBe an[AliasMapping]

        session.shutdown()
    }

    it should "support different outputs" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution

        val mapping = AliasMapping(
            Mapping.Properties(session.context),
            MappingOutputIdentifier("input_df:output_2")
        )

        val inputDf = spark.emptyDataFrame
        mapping.input should be (MappingOutputIdentifier("input_df:output_2"))
        mapping.inputs should be (Set(MappingOutputIdentifier("input_df:output_2")))
        mapping.outputs should be (Set("main"))

        val result = mapping.execute(executor, Map(MappingOutputIdentifier("input_df:output_2") -> inputDf))("main")
        result.count() should be (0)

        session.shutdown()
    }
}
