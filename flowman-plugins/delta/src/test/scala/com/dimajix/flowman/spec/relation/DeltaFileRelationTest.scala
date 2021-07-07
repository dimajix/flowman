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

package com.dimajix.flowman.spec.relation

import java.io.File
import java.io.FileNotFoundException
import java.nio.file.FileAlreadyExistsException

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.{types => ftypes}
import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.spark.testing.QueryTest


class DeltaFileRelationTest extends AnyFlatSpec with Matchers with LocalSparkSession with QueryTest {
    override def configureSpark(builder: SparkSession.Builder): SparkSession.Builder = {
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    }

    "The DeltaFileRelation" should "be parseable" in {
        val spec =
            """
              |kind: deltaFile
              |description: "Some Delta Table"
              |location: hdfs://ns/some/path
              |""".stripMargin

        val relationSpec = ObjectMapper.parse[RelationSpec](spec)
        relationSpec shouldBe a[DeltaFileRelationSpec]

        val session = Session.builder().build()
        val relation = relationSpec.instantiate(session.context).asInstanceOf[DeltaFileRelation]
        relation.description should be (Some("Some Delta Table"))
        relation.partitions should be (Seq())
        relation.location should be (new Path("hdfs://ns/some/path"))
        relation.options should be (Map())
        relation.properties should be (Map())
    }

    it should "support create/write/read/truncate/destroy with location" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val location = new File(tempDir, "delta/default/lala")
        val relation = DeltaFileRelation(
            Relation.Properties(context, "delta_relation"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context, "delta_schema"),
                fields = Seq(
                    Field("str_col", ftypes.StringType),
                    Field("int_col", ftypes.IntegerType)
                )
            )),
            location = new Path(location.toURI)
        )

        relation.fields should be (Seq(
            Field("str_col", ftypes.StringType),
            Field("int_col", ftypes.IntegerType)
        ))

        // == Create ===================================================================
        location.exists() should be (false)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution, false)
        location.exists() should be (true)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)

        // Try to create relation, although it already exists
        a[FileAlreadyExistsException] shouldBe thrownBy(relation.create(execution))
        relation.create(execution, true)

        // == Read ===================================================================
        relation.read(execution, None, Map()).count() should be (0)

        // == Write ===================================================================
        val schema = StructType(Seq(
            StructField("str_col", StringType),
            StructField("char_col", StringType),
            StructField("int_col", IntegerType)
        ))
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", "str", 21)
        ))
        val df = spark.createDataFrame(rdd, schema)
        relation.write(execution, df, Map())
        relation.loaded(execution, Map()) should be (Yes)

        // == Read ===================================================================
        val df2 = relation.read(execution, None, Map())
        val rows_1 = Seq(
            Row("v1", 21)
        )
        checkAnswer(df2, rows_1)

        // == Truncate ===================================================================
        relation.truncate(execution)
        location.exists() should be (true)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)

        // == Read ===================================================================
        relation.read(execution, None, Map()).count() should be (0)
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
        )))

        // == Destroy ===================================================================
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.destroy(execution)
        location.exists() should be (false)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)

        an[FileNotFoundException] shouldBe thrownBy(relation.destroy(execution))
        relation.destroy(execution, true)
    }

    it should "support read/write static partitions" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val location = new File(tempDir, "delta/default/lala2")
        val relation = DeltaFileRelation(
            Relation.Properties(context, "delta_relation"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context, "delta_schema"),
                fields = Seq(
                    Field("str_col", ftypes.StringType),
                    Field("int_col", ftypes.IntegerType)
                )
            )),
            location = new Path(location.toURI),
            partitions = Seq(
                PartitionField("part", ftypes.StringType)
            )
        )

        relation.fields should be (Seq(
            Field("str_col", ftypes.StringType),
            Field("int_col", ftypes.IntegerType),
            Field("part", ftypes.StringType, false)
        ))

        // == Create ===================================================================
        location.exists() should be (false)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
        relation.create(execution, false)
        location.exists() should be (true)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)

        // Try to create relation, although it already exists
        a[FileAlreadyExistsException] shouldBe thrownBy(relation.create(execution))
        relation.create(execution, true)

        // == Read ===================================================================
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("part", StringType, false)
        )))
        relation.read(execution, None, Map()).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Write ===================================================================
        val schema = StructType(Seq(
            StructField("str_col", StringType),
            StructField("char_col", StringType),
            StructField("int_col", IntegerType)
        ))
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", "str", 21)
        ))
        val df = spark.createDataFrame(rdd, schema)
        relation.write(execution, df, Map("part" -> SingleValue("p0")))
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)

        // == Read ===================================================================
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("part", StringType, false)
        )))
        relation.read(execution, None, Map()).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Overwrite ===================================================================
        relation.write(execution, df, Map("part" -> SingleValue("p0")))
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)

        // == Read ===================================================================
        relation.read(execution, None, Map()).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Write ===================================================================
        relation.write(execution, df, Map("part" -> SingleValue("p1")))
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (Yes)

        // == Read ===================================================================
        relation.read(execution, None, Map()).count() should be (2)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (1)

        // == Truncate ===================================================================
        relation.truncate(execution, Map("part" -> SingleValue("p0")))
        location.exists() should be (true)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (Yes)

        // == Read ===================================================================
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("part", StringType, false)
        )))
        relation.read(execution, None, Map()).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (1)

        // == Truncate ===================================================================
        relation.truncate(execution)
        location.exists() should be (true)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)

        // == Read ===================================================================
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("part", StringType, false)
        )))
        relation.read(execution, None, Map()).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Destroy ===================================================================
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.destroy(execution)
        location.exists() should be (false)
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)

        an[FileNotFoundException] shouldBe thrownBy(relation.destroy(execution))
        relation.destroy(execution, true)
    }

    it should "support read/write dynamic partitions" in {

    }

    it should "support append for unpartitioned tables" in {

    }

    it should "support append for partitioned tables" in {

    }

    it should "support read/write without schema" in {
        val session = Session.builder().withSparkSession(spark).build()
        val context = session.context
        val execution = session.execution

        val location = new File(tempDir, "delta/default/lala2")
        val relation0 = DeltaFileRelation(
            Relation.Properties(context, "delta_relation"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context, "delta_schema"),
                fields = Seq(
                    Field("str_col", ftypes.StringType),
                    Field("int_col", ftypes.IntegerType)
                )
            )),
            location = new Path(location.toURI),
            partitions = Seq(
                PartitionField("part", ftypes.StringType)
            )
        )

        // == Create =================================================================================================
        location.exists() should be (false)
        relation0.exists(execution) should be (No)
        relation0.loaded(execution, Map()) should be (No)
        relation0.create(execution, false)
        location.exists() should be (true)
        relation0.exists(execution) should be (Yes)
        relation0.loaded(execution, Map()) should be (No)
        relation0.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation0.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)

        // == Check =================================================================================================
        val relation = DeltaFileRelation(
            Relation.Properties(context, "delta_relation"),
            schema = None,
            location = new Path(location.toURI),
            partitions = Seq(
                PartitionField("part", ftypes.StringType)
            )
        )
        relation.schema should be (None)
        relation.fields should be (Seq(Field("part", ftypes.StringType, false)))
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)

        // == Read =================================================================================================
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("part", StringType, false)
        )))
        relation.read(execution, None, Map()).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Write =================================================================================================
        val schema = StructType(Seq(
            StructField("str_col", StringType),
            StructField("char_col", StringType),
            StructField("int_col", IntegerType)
        ))
        val rdd = spark.sparkContext.parallelize(Seq(
            Row("v1", "str", 21)
        ))
        val df = spark.createDataFrame(rdd, schema)
        relation0.write(execution, df, Map("part" -> SingleValue("p0")))

        // == Check =================================================================================================
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)

        // == Read ==================================================================================================
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("part", StringType, false)
        )))
        relation.read(execution, None, Map()).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Write external ========================================================================================
        relation.write(execution, df, Map("part" -> SingleValue("p1")))
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (Yes)

        // == Read ==================================================================================================
        relation0.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("part", StringType, false)
        )))
        relation0.read(execution, None, Map()).count() should be (2)
        relation0.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (1)
        relation0.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (1)

        // == Truncate =============================================================================================
        relation0.truncate(execution, Map("part" -> SingleValue("p0")))

        // == Check =================================================================================================
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (Yes)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (Yes)

        // == Read ==================================================================================================
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("part", StringType, false)
        )))
        relation.read(execution, None, Map()).count() should be (1)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (1)

        // == Truncate ==============================================================================================
        relation0.truncate(execution)

        // == Check =================================================================================================
        location.exists() should be (true)
        relation.exists(execution) should be (Yes)
        relation.loaded(execution, Map()) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p0"))) should be (No)
        relation.loaded(execution, Map("part" -> SingleValue("p1"))) should be (No)

        // == Read =================================================================================================
        relation.read(execution, None, Map()).schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType),
            StructField("part", StringType, false)
        )))
        relation.read(execution, None, Map()).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p0"))).count() should be (0)
        relation.read(execution, None, Map("part" -> SingleValue("p1"))).count() should be (0)

        // == Destroy ===============================================================================================
        relation0.destroy(execution)

        // == Check =================================================================================================
        relation.exists(execution) should be (No)
        relation.loaded(execution, Map()) should be (No)
    }
}
