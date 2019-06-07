/*
 * Copyright 2019 Kaya Kupferschmidt
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
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.testing.LocalSparkSession
import com.dimajix.flowman.{types => ftypes}


object UnionTransformerTest {
    case class Class1(col0:String, col1:Int)
    case class Class2(col0:String, col2:Boolean)
    case class Class3(col0:Double, col1:Int)
}

class UnionTransformerTest extends FlatSpec with Matchers with LocalSparkSession {
    import com.dimajix.flowman.transforms.UnionTransformerTest._

    "The UnionTransformer" should "work" in {
        val lspark = spark
        import lspark.implicits._

        val xfs = UnionTransformer()

        val df1 = Seq(Class1("x", 2)).toDF
        val df2 = Seq(Class2("x", false)).toDF
        val resultDf = xfs.transformDataFrames(Seq(df1, df2))
        resultDf.schema should be (
            StructType(Seq(
                StructField("col0", StringType, true),
                StructField("col1", IntegerType, true),
                StructField("col2", BooleanType, true)
            ))
        )

        val schema1 = ftypes.StructType.of(df1.schema)
        val schema2 = ftypes.StructType.of(df2.schema)
        val resultSchema = xfs.transformSchemas(Seq(schema1, schema2))
        resultSchema.sparkType should be (resultDf.schema)
    }
}
