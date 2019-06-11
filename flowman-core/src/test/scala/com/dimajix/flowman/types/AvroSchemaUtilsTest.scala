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

import scala.collection.JavaConversions._

import org.apache.avro.Schema.Type._
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class AvroSchemaUtilsTest extends FlatSpec with Matchers {
    "AvroSchemaUtils" should "convert a Flowman schema of primitives to an Avro schema" in {
        val schema = Seq(
            Field("str_field", StringType, false),
            Field("int_field", IntegerType, false),
            Field("long_field", LongType, false),
            Field("short_field", ShortType, false),
            Field("bool_field", BooleanType, false),
            Field("bin_field", BinaryType, false),
            Field("char_field", CharType(10), false),
            Field("varchar_field", VarcharType(10), false),
            Field("float_field", FloatType, false),
            Field("double_field", DoubleType, false),
            Field("date_field", DateType, false),
            Field("timestamp_field", TimestampType, false)
        )

        val result = AvroSchemaUtils.toAvro(schema)
        result.getType should be (RECORD)
        val fields = result.getFields()
        fields(0).schema().getType should be (STRING)
        fields(0).name() should be ("str_field")
        fields(1).schema().getType should be (INT)
        fields(1).name() should be ("int_field")
        fields(2).schema().getType should be (LONG)
        fields(2).name() should be ("long_field")
        fields(3).schema().getType should be (INT)
        fields(3).name() should be ("short_field")
        fields(4).schema().getType should be (BOOLEAN)
        fields(4).name() should be ("bool_field")
        fields(5).schema().getType should be (BYTES)
        fields(5).name() should be ("bin_field")
        fields(6).schema().getType should be (STRING)
        fields(6).name() should be ("char_field")
        fields(7).schema().getType should be (STRING)
        fields(7).name() should be ("varchar_field")
        fields(8).schema().getType should be (FLOAT)
        fields(8).name() should be ("float_field")
        fields(9).schema().getType should be (DOUBLE)
        fields(9).name() should be ("double_field")
        fields(10).schema().getType should be (LONG)
        fields(10).name() should be ("date_field")
        fields(11).schema().getType should be (LONG)
        fields(11).name() should be ("timestamp_field")
    }

    it should "support nullable fields via unions" in {
        val schema = Seq(
            Field("str_field", StringType, true)
        )

        val result = AvroSchemaUtils.toAvro(schema)
        result.getType should be (RECORD)
        val fields = result.getFields()
        fields(0).schema().getType should be (UNION)
        fields(0).name() should be ("str_field")
        fields(0).schema().getTypes().get(0).getType should be (STRING)
        fields(0).schema().getTypes().get(1).getType should be (NULL)
    }

    it should "support non-nullable arrays" in {
        val schema = Seq(
            Field("array_field", ArrayType(StringType, false), false)
        )

        val result = AvroSchemaUtils.toAvro(schema)
        result.getType should be (RECORD)
        val fields = result.getFields()
        fields(0).schema().getType should be (ARRAY)
        fields(0).name() should be ("array_field")
        fields(0).schema().getElementType.getType should be (STRING)
    }

    it should "support nullable arrays" in {
        val schema = Seq(
            Field("array_field", ArrayType(StringType, true), false)
        )

        val result = AvroSchemaUtils.toAvro(schema)
        result.getType should be (RECORD)
        val fields = result.getFields()
        fields(0).schema().getType should be (ARRAY)
        fields(0).name() should be ("array_field")
        fields(0).schema().getElementType.getType should be (UNION)
        fields(0).schema().getElementType.getTypes().get(0).getType should be (STRING)
        fields(0).schema().getElementType.getTypes().get(1).getType should be (NULL)
    }

    it should "create an Avro schema with a toString method" in {
        val schema = Seq(
            Field("str_field", StringType, false),
            Field("int_field", IntegerType, true)
        )

        val result = AvroSchemaUtils.toAvro(schema)
        result.toString(true).replace("\r\n", "\n") should be (
            """{
              |  "type" : "record",
              |  "name" : "topLevelRecord",
              |  "fields" : [ {
              |    "name" : "str_field",
              |    "type" : "string"
              |  }, {
              |    "name" : "int_field",
              |    "type" : [ "int", "null" ]
              |  } ]
              |}""".stripMargin)
    }

    it should "support doc strings" in {
        val schema = Seq(
            Field("str_field", StringType, false, Some("This is a doc")),
            Field("int_field", IntegerType, true, Some("This is a nullable doc"))
        )

        val result = AvroSchemaUtils.toAvro(schema)
        result.toString(true).replace("\r\n", "\n") should be (
            """{
              |  "type" : "record",
              |  "name" : "topLevelRecord",
              |  "fields" : [ {
              |    "name" : "str_field",
              |    "type" : "string",
              |    "doc" : "This is a doc"
              |  }, {
              |    "name" : "int_field",
              |    "type" : [ "int", "null" ],
              |    "doc" : "This is a nullable doc"
              |  } ]
              |}""".stripMargin)
    }

    it should "correctly support a round trip (1)" in {
        val spec =
            """{
              |  "type" : "record",
              |  "name" : "topLevelRecord",
              |  "fields" : [ {
              |    "name" : "AgencyDossierNumber",
              |    "type" : [ "string", "null" ]
              |  }, {
              |    "name" : "AggregatedFields",
              |    "type" : [ {
              |      "type" : "record",
              |      "name" : "AggregatedFields",
              |      "namespace" : ".AggregatedFields",
              |      "fields" : [ {
              |        "name" : "Traveler",
              |        "type" : [ {
              |          "type" : "record",
              |          "name" : "Traveler",
              |          "namespace" : ".AggregatedFields.Traveler",
              |          "fields" : [ {
              |            "name" : "Latin",
              |            "type" : [ "string", "null" ]
              |          } ]
              |        }, "null" ]
              |      } ]
              |    }, "null" ]
              |  } ]
              |}""".stripMargin
        val avroSchema = new org.apache.avro.Schema.Parser().parse(spec)
        val fields = AvroSchemaUtils.fromAvro(avroSchema)
        val result = AvroSchemaUtils.toAvro(fields)
        result.toString(true).replace("\r\n", "\n") should be (spec)
    }

    it should "correctly support a round trip (2)" in {
        val spec =
            """{
              |  "type" : "record",
              |  "name" : "topLevelRecord",
              |  "fields" : [ {
              |    "name" : "CommissionPassback",
              |    "type" : [ {
              |      "type" : "array",
              |      "items" : [ {
              |        "type" : "record",
              |        "name" : "CommissionPassback",
              |        "namespace" : ".CommissionPassback",
              |        "fields" : [ {
              |          "name" : "Description",
              |          "type" : [ {
              |            "type" : "array",
              |            "items" : [ {
              |              "type" : "record",
              |              "name" : "Description",
              |              "namespace" : ".CommissionPassback.Description",
              |              "fields" : [ {
              |                "name" : "Latin",
              |                "type" : [ "string", "null" ]
              |              }, {
              |                "name" : "num",
              |                "type" : [ "long", "null" ]
              |              } ]
              |            }, "null" ]
              |          }, "null" ]
              |        }, {
              |          "name" : "OriginalAgencyDocumentNumber",
              |          "type" : [ "string", "null" ]
              |        }, {
              |          "name" : "Traveler",
              |          "type" : [ {
              |            "type" : "record",
              |            "name" : "Traveler",
              |            "namespace" : ".CommissionPassback.Traveler",
              |            "fields" : [ {
              |              "name" : "FullName",
              |              "type" : [ {
              |                "type" : "record",
              |                "name" : "FullName",
              |                "namespace" : ".CommissionPassback.Traveler.FullName",
              |                "fields" : [ {
              |                  "name" : "Latin",
              |                  "type" : [ "string", "null" ]
              |                } ]
              |              }, "null" ]
              |            } ]
              |          }, "null" ]
              |        }, {
              |          "name" : "uid",
              |          "type" : [ "string", "null" ]
              |        } ]
              |      }, "null" ]
              |    }, "null" ]
              |  } ]
              |}""".stripMargin
        val avroSchema = new org.apache.avro.Schema.Parser().parse(spec)
        val fields = AvroSchemaUtils.fromAvro(avroSchema)
        val result = AvroSchemaUtils.toAvro(fields)
        result.toString(true).replace("\r\n", "\n") should be (spec)
    }
}
