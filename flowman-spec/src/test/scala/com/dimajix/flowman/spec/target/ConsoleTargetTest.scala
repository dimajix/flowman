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

package com.dimajix.flowman.spec.target

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.dataset.ValuesDataset
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class ConsoleTargetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "A ConsoleTarget" should "be parseable" in {
        val spec =
            """
              |targets:
              |  custom:
              |    kind: console
              |    input:
              |      kind: mapping
              |      mapping: some_mapping
              |    limit: 10
              |    columns: [col_a,col_b]
              |    csv: false
              |    header: true
              |""".stripMargin

        val module = Module.read.string(spec)
        val target = module.targets("custom")
        target shouldBe an[ConsoleTargetSpec]
    }

    it should "print records onto the console" in {
        val session = Session.builder.withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val schema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))
        val dataset = ValuesDataset(
            Dataset.Properties(context, "const"),
            columns = schema.fields,
            records = Seq(
                Array("lala","12"),
                Array("lolo","13"),
                Array("",null)
            )
        )
        val target = ConsoleTarget(
            Target.Properties(context),
            dataset,
            10,
            true,
            false,
            Seq()
        )

        target.phases should be (Set(Phase.BUILD))
        target.requires(Phase.BUILD) should be (Set())
        target.provides(Phase.BUILD) should be (Set())
        target.before should be (Seq())
        target.after should be (Seq())

        target.dirty(execution, Phase.BUILD) should be (Yes)
        target.execute(execution, Phase.BUILD)
    }
}
