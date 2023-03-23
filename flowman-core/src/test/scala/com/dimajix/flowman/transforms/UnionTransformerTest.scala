/*
 * Copyright (C) 2019 The Flowman Authors
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

package com.dimajix.flowman.transforms

import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession


object UnionTransformerTest {
    case class Class1(colx:String, col0:String, col1:Int, col2:Int)
    case class Class2(coly:String, coly0:String, col0:String, col0a:Int, col0b:Int, col1:Int, col3:Boolean)
    case class Class3(col0:Double, col1:Int)
}

class UnionTransformerTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    import com.dimajix.flowman.transforms.UnionTransformerTest._

    "The UnionTransformer" should "work" in {
        val lspark = spark
        import lspark.implicits._

        val xfs = UnionTransformer()

        val df1 = Seq(Class1("x", "y", 2, 3)).toDF
        val df2 = Seq(Class2("y", "y0", "y", 2, 3, 4, false)).toDF
        val resultDf = xfs.transformDataFrames(Seq(df1, df2))
        resultDf.schema should be (
            StructType(Seq(
                StructField("colx", StringType, true),
                StructField("coly", StringType, true),
                StructField("coly0", StringType, true),
                StructField("col0", StringType, true),
                StructField("col0a", IntegerType, true),
                StructField("col0b", IntegerType, true),
                StructField("col1", IntegerType, false),
                StructField("col3", BooleanType, true),
                StructField("col2", IntegerType, true)
            ))
        )

        val schema1 = ftypes.StructType.of(df1.schema)
        val schema2 = ftypes.StructType.of(df2.schema)
        val resultSchema = xfs.transformSchemas(Seq(schema1, schema2))
        resultSchema.sparkType should be (resultDf.schema)
    }
}
