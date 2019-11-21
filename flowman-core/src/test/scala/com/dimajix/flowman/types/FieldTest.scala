/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.types

import org.apache.spark.sql.types.MetadataBuilder
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ObjectMapper


class FieldTest extends FlatSpec with Matchers {
    lazy val mapper = ObjectMapper

    "A Field" should "be nullable per default" in {
        val spec =
            """
              |name: lala
              |type: String
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkField should be (org.apache.spark.sql.types.StructField("lala", org.apache.spark.sql.types.StringType, true))
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }
    it should "be nullable if explicitly said so" in {
        val spec =
            """
              |name: lala
              |type: String
              |nullable: true
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkField should be (org.apache.spark.sql.types.StructField("lala", org.apache.spark.sql.types.StringType, true))
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }
    it should "not be nullable if explicitly said so" in {
        val spec =
            """
              |name: lala
              |type: String
              |nullable: false
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (false)
        result.name should be ("lala")
        result.sparkField should be (org.apache.spark.sql.types.StructField("lala", org.apache.spark.sql.types.StringType, false))
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }

    it should "support documentation" in {
        val spec =
            """
              |name: lala
              |type: String
              |description: Some description
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.description should be (Some("Some description"))
        result.sparkField should be (org.apache.spark.sql.types.StructField("lala", org.apache.spark.sql.types.StringType, true).withComment("Some description"))
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }

    it should "support sizes" in {
        val spec =
            """
              |name: lala
              |type: String
              |size: 27
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.size should be (Some(27))
        result.sparkField should be (org.apache.spark.sql.types.StructField("lala", org.apache.spark.sql.types.StringType, true, new MetadataBuilder().putLong("size", 27).build()))
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }

    it should "support default values" in {
        val spec =
            """
              |name: lala
              |type: String
              |default: 27
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.default should be (Some("27"))
        result.sparkField should be (org.apache.spark.sql.types.StructField("lala", org.apache.spark.sql.types.StringType, true, new MetadataBuilder().putString("default", "27").build()))
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }

    it should "support empty default values" in {
        val spec =
            """
              |name: lala
              |type: String
              |default:
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        if (result.default == Some("")) {
            result.default should be(Some(""))
            result.sparkField should be(org.apache.spark.sql.types.StructField("lala", org.apache.spark.sql.types.StringType, true, new MetadataBuilder().putString("default", "").build()))
            result.sparkType should be(org.apache.spark.sql.types.StringType)
            result.ftype should be(StringType)
        }
        else {
            result.default should be(None)
            result.sparkField should be(org.apache.spark.sql.types.StructField("lala", org.apache.spark.sql.types.StringType, true, new MetadataBuilder().build()))
            result.sparkType should be(org.apache.spark.sql.types.StringType)
            result.ftype should be(StringType)
        }
    }

    it should "support null default values" in {
        val spec =
            """
              |name: lala
              |type: String
              |default: null
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.default should be (None)
        result.sparkField should be (org.apache.spark.sql.types.StructField("lala", org.apache.spark.sql.types.StringType, true, new MetadataBuilder().build()))
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }

    it should "support empty string default values" in {
        val spec =
            """
              |name: lala
              |type: String
              |default: ""
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.default should be (Some(""))
        result.sparkField should be (org.apache.spark.sql.types.StructField("lala", org.apache.spark.sql.types.StringType, true, new MetadataBuilder().putString("default", "").build()))
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }

    "A string Field" should "be deserializable" in {
        val spec =
            """
              |name: lala
              |type: String
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }
    it should "be deserializable as text" in {
        val spec =
            """
              |name: lala
              |type: TEXT
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (StringType)
    }

    "A struct Field" should "be deserializable" in {
        val spec =
            """
              |name: lala
              |type:
              |  kind: struct
              |  fields:
              |    - name: lolo
              |      type: String
              |      nullable: false
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType shouldBe a[org.apache.spark.sql.types.StructType]
        result.sparkType should be (org.apache.spark.sql.types.StructType(
            org.apache.spark.sql.types.StructField("lolo", org.apache.spark.sql.types.StringType, false) :: Nil
        ))
        result.ftype shouldBe a[StructType]
        result.ftype.asInstanceOf[StructType].fields(0).name should be ("lolo")
        result.ftype.asInstanceOf[StructType].fields(0).ftype should be (StringType)
        result.ftype.asInstanceOf[StructType].fields(0).sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype.asInstanceOf[StructType].fields(0).nullable should be (false)
    }

    "An array Field" should "be deserializable" in {
        val spec =
            """
              |name: lala
              |type:
              |  kind: array
              |  elementType: String
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType shouldBe a[org.apache.spark.sql.types.ArrayType]
        result.sparkType should be (org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType, true))
    }
    it should "be deserializable with nullable elements" in {
        val spec =
            """
              |name: lala
              |type:
              |  kind: array
              |  containsNull: true
              |  elementType: String
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType shouldBe a[org.apache.spark.sql.types.ArrayType]
        result.sparkType should be (org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType, true))
    }
    it should "be deserializable with non-nullable elements" in {
        val spec =
            """
              |name: lala
              |type:
              |  kind: array
              |  containsNull: false
              |  elementType: String
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType shouldBe a[org.apache.spark.sql.types.ArrayType]
        result.sparkType should be (org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType, false))
    }

    "An array Field of Structs" should "be deserializable" in {
        val spec =
            """
              |name: lala
              |type:
              |  kind: array
              |  elementType:
              |    kind: struct
              |    fields:
              |      - name: lolo
              |        type: string
              |      - name: i
              |        type: Integer
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.ftype shouldBe a[ArrayType]
        result.ftype.asInstanceOf[ArrayType].elementType shouldBe a[StructType]
        result.sparkType shouldBe a[org.apache.spark.sql.types.ArrayType]
        result.sparkType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType shouldBe a[org.apache.spark.sql.types.StructType]
        val struct = result.ftype.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
        struct.fields.size should be (2)
        struct.fields(0).name should be ("lolo")
        struct.fields(0).ftype should be (StringType)
        struct.fields(0).sparkType should be (org.apache.spark.sql.types.StringType)
        struct.fields(0).nullable should be (true)
        struct.fields(1).name should be ("i")
        struct.fields(1).ftype should be (IntegerType)
        struct.fields(1).sparkType should be (org.apache.spark.sql.types.IntegerType)
        struct.fields(1).nullable should be (true)
    }

    "A decimal Field" should "be deserializable" in {
        val spec =
            """
              |name: lala
              |type: decimal(10,4)
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType should be (org.apache.spark.sql.types.DecimalType(10,4))
        result.ftype should be (DecimalType(10,4))
    }

    "A varchar Field" should "be deserializable" in {
        val spec =
            """
              |name: lala
              |type: varchar(14)
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (VarcharType(14))
    }

    "A char Field" should "be deserializable" in {
        val spec =
            """
              |name: lala
              |type: char(14)
            """.stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = mapper.parse[Field](spec)
        result.nullable should be (true)
        result.name should be ("lala")
        result.sparkType should be (org.apache.spark.sql.types.StringType)
        result.ftype should be (CharType(14))
    }
}
