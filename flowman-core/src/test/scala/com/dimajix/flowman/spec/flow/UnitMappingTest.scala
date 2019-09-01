/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.flow

import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Module
import com.dimajix.spark.testing.LocalSparkSession


class UnitMappingTest extends FlatSpec with Matchers with LocalSparkSession {
    private var inputDf0 : DataFrame = _
    private var inputDf1 : DataFrame = _

    override def beforeAll(): Unit = {
        super.beforeAll()

        val spark = this.spark
        import spark.implicits._

        val inputRecords0 = Seq((100, "one hundred"))
        inputDf0 = spark.createDataset(inputRecords0).toDF()
        inputDf0.createOrReplaceTempView("t0")

        val inputRecords1 = Seq((200, "two hundred"))
        inputDf1 = spark.createDataset(inputRecords1).toDF()
        inputDf1.createOrReplaceTempView("t1")
    }


    "A UnitMapping" should "not reuse private data frames" in {
        val spec =
            """
              |mappings:
              |  macro:
              |    kind: unit
              |    mappings:
              |      input:
              |        kind: provided
              |        table: ${table}
              |
              |  instance_0:
              |    kind: template
              |    mapping: macro
              |    environment:
              |     - table=t0
              |
              |  instance_1:
              |    kind: template
              |    mapping: macro
              |    environment:
              |     - table=t1
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.executor

        val instance0 = context.getMapping(MappingIdentifier("instance_0"))
        instance0.inputs should be (Seq())
        instance0.outputs should be (Seq("input"))
        val df0 = executor.instantiate(instance0, "input")
        df0.collect() should be (inputDf0.collect())

        val instance1 = context.getMapping(MappingIdentifier("instance_1"))
        instance1.inputs should be (Seq())
        instance1.outputs should be (Seq("input"))
        val df1 = executor.instantiate(instance1, "input")
        df1.collect() should be (inputDf1.collect())
    }

    it should "work with internal and external references" in {
        val spec = """
          |mappings:
          |  outside:
          |    kind: provided
          |    table: t0
          |
          |  macro:
          |    kind: unit
          |    mappings:
          |      inside:
          |        kind: provided
          |        table: t0
          |
          |      output:
          |        kind: union
          |        inputs:
          |         - inside
          |         - outside
          |""".stripMargin

        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.executor

        val unit = context.getMapping(MappingIdentifier("macro"))
        unit.inputs should be (Seq(MappingOutputIdentifier("outside")))
        unit.outputs.sorted should be (Seq("inside", "output"))

        val df_inside = executor.instantiate(unit, "inside")
        df_inside.collect() should be (inputDf0.collect())

        val df_outside = executor.instantiate(unit, "output")
        df_outside.collect() should be (inputDf0.union(inputDf0).collect())
    }

    it should "work nicely with alias" in {
        val spec =
            """
              |mappings:
              |  macro:
              |    kind: unit
              |    mappings:
              |      input:
              |        kind: provided
              |        table: t0
              |
              |  alias:
              |    kind: alias
              |    input: macro:input
              |""".stripMargin

        val project = Module.read.string(spec).toProject("project")

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.getContext(project)
        val executor = session.executor

        val instance0 = context.getMapping(MappingIdentifier("alias"))
        instance0.inputs should be (Seq(MappingOutputIdentifier("macro:input")))
        instance0.outputs should be (Seq("main"))
        val df0 = executor.instantiate(instance0, "main")
        df0.collect() should be (inputDf0.collect())
    }
}
