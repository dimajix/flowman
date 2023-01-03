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

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class UnitMappingTest extends AnyFlatSpec with Matchers with LocalSparkSession {
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
        val executor = session.execution

        val instance0 = context.getMapping(MappingIdentifier("instance_0"))
        instance0.inputs should be (Set.empty)
        instance0.outputs should be (Set("input"))
        val df0 = executor.instantiate(instance0, "input")
        df0.collect() should be (inputDf0.collect())
        val schema0 = executor.describe(instance0, "input")
        schema0 should be (StructType(Seq(Field("_1", IntegerType, false), Field("_2", StringType, true))))

        val instance1 = context.getMapping(MappingIdentifier("instance_1"))
        instance1.inputs should be (Set.empty)
        instance1.outputs should be (Set("input"))
        val df1 = executor.instantiate(instance1, "input")
        df1.collect() should be (inputDf1.collect())
        val schema1 = executor.describe(instance1, "input")
        schema1 should be (StructType(Seq(Field("_1", IntegerType, false), Field("_2", StringType, true))))

        session.shutdown()
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
        val executor = session.execution

        val unit = context.getMapping(MappingIdentifier("macro"))
        unit.inputs should be (Set(MappingOutputIdentifier("outside")))
        unit.outputs should be (Set("inside", "output"))

        val df_inside = executor.instantiate(unit, "inside")
        df_inside.collect() should be (inputDf0.collect())
        val schema_inside = executor.describe(unit, "inside")
        schema_inside should be (StructType(Seq(Field("_1", IntegerType, false), Field("_2", StringType, true))))

        val df_outside = executor.instantiate(unit, "output")
        df_outside.collect() should be (inputDf0.union(inputDf0).collect())
        val schema_outside = executor.describe(unit, "output")
        schema_outside should be (StructType(Seq(Field("_1", IntegerType, false), Field("_2", StringType, true))))

        session.shutdown()
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
        val executor = session.execution

        val instance0 = context.getMapping(MappingIdentifier("alias"))
        instance0.inputs should be (Set(MappingOutputIdentifier("macro:input")))
        instance0.outputs should be (Set("main"))
        val df0 = executor.instantiate(instance0, "main")
        df0.collect() should be (inputDf0.collect())

        session.shutdown()
    }

    it should "work with complex schema inference" in {
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
                     |      conformed:
                     |        kind: schema
                     |        input: inside
                     |        schema:
                     |          kind: mapping
                     |          mapping: output
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
        val executor = session.execution

        val unit = context.getMapping(MappingIdentifier("macro"))
        unit.inputs should be (Set(MappingOutputIdentifier("outside")))
        unit.outputs should be (Set("inside", "conformed", "output"))

        val df_inside = executor.instantiate(unit, "inside")
        df_inside.collect() should be (inputDf0.collect())
        val schema_inside = executor.describe(unit, "inside")
        schema_inside should be (StructType(Seq(Field("_1", IntegerType, false), Field("_2", StringType, true))))

        val df_outside = executor.instantiate(unit, "output")
        df_outside.collect() should be (inputDf0.union(inputDf0).collect())
        val schema_outside = executor.describe(unit, "output")
        schema_outside should be (StructType(Seq(Field("_1", IntegerType, false), Field("_2", StringType, true))))

        val df_conformed = executor.instantiate(unit, "conformed")
        df_conformed.collect() should be (inputDf0.collect())
        val schema_conformed = executor.describe(unit, "conformed")
        schema_conformed should be (StructType(Seq(Field("_1", IntegerType, false), Field("_2", StringType, true))))

        session.shutdown()
    }
}
