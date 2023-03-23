/*
 * Copyright (C) 2021 The Flowman Authors
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

import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types.ArrayRecord
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalSparkSession


class ValuesDatasetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The ValuesDataset" should "be parsable with columns" in {
        val spec =
            """
              |kind: values
              |columns:
              |  col_a: string
              |  col_b: int
              |records:
              |  - ["a",1]
              |  - ["b",2]
              |""".stripMargin

        val session = Session.builder().disableSpark().build()
        val context = session.context

        val ds = ObjectMapper.parse[DatasetSpec](spec)
        ds shouldBe a[ValuesDatasetSpec]

        val dataset = ds.instantiate(context).asInstanceOf[ValuesDataset]
        dataset.category should be (Category.DATASET)
        dataset.kind should be ("values")
        dataset.records should be (Seq(
            ArrayRecord("a","1"),
            ArrayRecord("b", "2")
        ))

        session.shutdown()
    }

    it should "be parseable with a schema" in {
        val spec =
            """
              |kind: values
              |records:
              |  - ["a",12,3]
              |  - [cat,"",7]
              |  - [dog,null,8]
              |schema:
              |  kind: inline
              |  fields:
              |    - name: str_col
              |      type: string
              |    - name: int_col
              |      type: integer
              |""".stripMargin

        val session = Session.builder().disableSpark().build()
        val context = session.context

        val ds = ObjectMapper.parse[DatasetSpec](spec)
        ds shouldBe a[ValuesDatasetSpec]

        val dataset = ds.instantiate(context).asInstanceOf[ValuesDataset]

        dataset.category should be (Category.DATASET)
        dataset.kind should be ("values")
        dataset.records should be (Seq(
            ArrayRecord("a","12","3"),
            ArrayRecord("cat","","7"),
            ArrayRecord("dog",null,"8")
        ))

        session.shutdown()
    }

    it should "work with specified records and schema" in {
        val schema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val executor = session.execution

        val dataset = ValuesDataset(
            Dataset.Properties(context, "const"),
            schema = Some(InlineSchema(
                Schema.Properties(context),
                fields = schema.fields
            )),
            records = Seq(
                ArrayRecord("lala","12"),
                ArrayRecord("lolo","13"),
                ArrayRecord("",null)
            )
        )

        dataset.describe(executor) should be (Some(schema))
        dataset.exists(executor) should be (Yes)
        dataset.requires should be (Set())
        dataset.provides should be (Set())
        an[UnsupportedOperationException] should be thrownBy(dataset.clean(executor))
        an[UnsupportedOperationException] should be thrownBy(dataset.write(executor, spark.emptyDataFrame))

        val df = dataset.read(executor)
        df.schema should be (schema.sparkType)
        df.collect() should be (Seq(
            Row("lala", 12),
            Row("lolo", 13),
            Row(null,null)
        ))

        session.shutdown()
    }

    it should "work with specified records and columns" in {
        val schema = new StructType(Seq(
            Field("str_col", StringType),
            Field("int_col", IntegerType)
        ))

        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val executor = session.execution

        val dataset = ValuesDataset(
            Dataset.Properties(context, "const"),
            columns = schema.fields,
            records = Seq(
                ArrayRecord("lala","12"),
                ArrayRecord("lolo","13"),
                ArrayRecord("",null)
            )
        )

        dataset.describe(executor) should be (Some(schema))
        dataset.exists(executor) should be (Yes)
        dataset.requires should be (Set())
        dataset.provides should be (Set())
        an[UnsupportedOperationException] should be thrownBy(dataset.clean(executor))
        an[UnsupportedOperationException] should be thrownBy(dataset.write(executor, spark.emptyDataFrame))

        val df = dataset.read(executor)
        df.schema should be (schema.sparkType)
        df.collect() should be (Seq(
            Row("lala", 12),
            Row("lolo", 13),
            Row(null,null)
        ))

        session.shutdown()
    }
}
