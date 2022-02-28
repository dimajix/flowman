/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.relation.HiveTableRelation
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.types.Field
import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.flowman.{types => ftypes}


class ReadHiveTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "A ReadHiveMapping" should "be parseable" in {
        val spec =
            """
              |mappings:
              |  t0:
              |    kind: readHive
              |    database: default
              |    table: t0
              |    filter: "landing_date > 123"
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().withProject(project).disableSpark().build()
        val context = session.getContext(project)
        val mapping = context.getMapping(MappingIdentifier("t0"))

        mapping shouldBe a[ReadHiveMapping]
        val rrm = mapping.asInstanceOf[ReadHiveMapping]
        rrm.table should be (TableIdentifier("t0", Some("default")))
        rrm.filter should be (Some("landing_date > 123"))
    }

    it should "work without columns" in (if (hiveSupported) {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "t0"),
            table = TableIdentifier("lala_0007", Some("default")),
            format = Some("parquet"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", ftypes.StringType),
                    Field("int_col", ftypes.IntegerType)
                )
            ))
        )
        relation.create(execution)

        val mapping = ReadHiveMapping(
            Mapping.Properties(context, "readHive"),
            TableIdentifier("lala_0007", Some("default"))
        )

        mapping.requires should be (Set(
            ResourceIdentifier.ofHiveTable("lala_0007", Some("default")),
            ResourceIdentifier.ofHiveDatabase("default")
        ))
        mapping.inputs should be (Set())
        mapping.describe(execution, Map()) should be (Map(
            "main" -> ftypes.StructType(Seq(
                Field("str_col", ftypes.StringType),
                Field("int_col", ftypes.IntegerType)
            ))
        ))

        val df = mapping.execute(execution, Map())("main")
        df.schema should be (StructType(Seq(
            StructField("str_col", StringType),
            StructField("int_col", IntegerType)
        )))

        relation.destroy(execution)
    })

    it should "work with columns" in (if (hiveSupported) {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution
        val context = session.context

        val relation = HiveTableRelation(
            Relation.Properties(context, "t0"),
            table = TableIdentifier("lala_0007", Some("default")),
            format = Some("parquet"),
            schema = Some(EmbeddedSchema(
                Schema.Properties(context),
                fields = Seq(
                    Field("str_col", ftypes.StringType),
                    Field("int_col", ftypes.IntegerType)
                )
            ))
        )
        relation.create(execution)

        val mapping = ReadHiveMapping(
            Mapping.Properties(context, "readHive"),
            table = TableIdentifier("lala_0007", Some("default")),
            columns = Seq(
                Field("int_col", ftypes.DoubleType)
            )
        )

        mapping.requires should be (Set(
            ResourceIdentifier.ofHiveTable("lala_0007", Some("default")),
            ResourceIdentifier.ofHiveDatabase("default")
        ))
        mapping.inputs should be (Set())
        mapping.describe(execution, Map()) should be (Map(
            "main" -> ftypes.StructType(Seq(
                Field("int_col", ftypes.DoubleType)
            ))
        ))

        val df = mapping.execute(execution, Map())("main")
        df.schema should be (StructType(Seq(
            StructField("int_col", DoubleType)
        )))

        relation.destroy(execution)
    })
}
