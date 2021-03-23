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

package com.dimajix.flowman.spec.schema

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.CharType
import com.dimajix.flowman.types.DateType
import com.dimajix.flowman.types.DecimalType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.TimestampType
import com.dimajix.flowman.types.VarcharType


class SwaggerSchemaUtilsTest extends AnyFlatSpec with Matchers  {
    "A Swagger Schema" should "be deserializable" in {
        val spec =
            """
              |swagger: "2.0"
              |info:
              |  version: 1.0.0
              |  title: Swagger Petstore
              |  description: A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification
              |  termsOfService: http://swagger.io/terms/
              |  contact:
              |    name: Swagger API Team
              |    email: apiteam@swagger.io
              |    url: http://swagger.io
              |  license:
              |    name: Apache 2.0
              |url: https://www.apache.org/licenses/LICENSE-2.0.html
              |definitions:
              |  Pet:
              |    required:
              |      - name
              |      - id
              |    properties:
              |      name:
              |        type: string
              |        description: The Pets name
              |      tag:
              |        type: string
              |      id:
              |        type: integer
              |        format: int64
              |        description: The Pets ID
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("Pet"), false)
        fields.size should be (3)

        fields(0).nullable should be (false)
        fields(0).name should be ("name")
        fields(0).description should be (Some("The Pets name"))
        fields(0).ftype should be (StringType)

        fields(1).nullable should be (true)
        fields(1).name should be ("tag")
        fields(1).ftype should be (StringType)

        fields(2).nullable should be (false)
        fields(2).name should be ("id")
        fields(2).description should be (Some("The Pets ID"))
        fields(2).ftype should be (LongType)
    }

    it should "support null value for required" in {
        val spec =
            """
              |swagger: "2.0"
              |info:
              |  version: 1.0.0
              |  title: Swagger Petstore
              |  description: A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification
              |  termsOfService: http://swagger.io/terms/
              |  contact:
              |    name: Swagger API Team
              |    email: apiteam@swagger.io
              |    url: http://swagger.io
              |  license:
              |    name: Apache 2.0
              |url: https://www.apache.org/licenses/LICENSE-2.0.html
              |definitions:
              |  Pet:
              |    required: null
              |    properties:
              |      name:
              |        type: string
              |        description: The Pets name
              |      tag:
              |        type: object
              |        required: null
              |        properties:
              |          some_int:
              |            type: integer
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("Pet"), false)
        fields should be (Seq(
            Field("name", StringType, description = Some("The Pets name")),
            Field("tag", StructType(Seq(
                Field("some_int", IntegerType)
            )))
        ))
    }

    it should "support timestamps and dates" in {
        val spec =
            """
              |swagger: "2.0"
              |definitions:
              |  SomeObject:
              |    type: object
              |    properties:
              |      ts:
              |        type: string
              |        format: date-time
              |      dt:
              |        type: string
              |        format: date
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("SomeObject"), false)
        fields should be (Seq(
            Field("ts", TimestampType, format=Some("date-time")),
            Field("dt", DateType, format=Some("date"))
        ))
    }

    it should "support simple arrays" in {
        val spec =
            """
              |swagger: "2.0"
              |definitions:
              |  Address:
              |    type: object
              |    properties:
              |      AddressLine:
              |        type: "array"
              |        items:
              |          type: string
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("Address"), false)
        fields should be (Seq(
            Field("AddressLine", ArrayType(StringType))
        ))
    }

    it should "support struct arrays" in {
        val spec =
            """
              |swagger: "2.0"
              |definitions:
              |  Address:
              |    description: "Service Provider's address"
              |    additionalProperties: false
              |    type: object
              |    properties:
              |      num:
              |        type: "integer"
              |        maximum: 255
              |        minimum: 0
              |      AddressLine:
              |        type: "array"
              |        items:
              |          description: "One of multiple address lines"
              |          type: "object"
              |          required:
              |            - "Latin"
              |          properties:
              |            Latin:
              |              type: "string"
              |              example: "John Doe/MR"
              |            LocalSpelling:
              |              type: "string"
              |              example: "李四/ MR"
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("Address"), false)
        fields should be (Seq(
            Field("num", IntegerType),
            Field("AddressLine", ArrayType(
                StructType(Seq(
                    Field("Latin", StringType, false),
                    Field("LocalSpelling", StringType)
                ))
            ))
        ))
    }

    it should "support allOf for properties" in {
        val spec =
            """
              |swagger: "2.0"
              |info:
              |  version: 1.0.0
              |  title: Swagger Petstore
              |  description: A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification
              |  termsOfService: http://swagger.io/terms/
              |  contact:
              |    name: Swagger API Team
              |    email: apiteam@swagger.io
              |    url: http://swagger.io
              |  license:
              |    name: Apache 2.0
              |url: https://www.apache.org/licenses/LICENSE-2.0.html
              |definitions:
              |  Pet:
              |    allOf:
              |      -
              |        type: object
              |        description: Some Pet object primary properties
              |        required:
              |          - name
              |        properties:
              |          name:
              |            type: string
              |            description: The Pets name
              |          tag:
              |            type: string
              |      -
              |        type: object
              |        required:
              |          - id
              |        properties:
              |          id:
              |            type: integer
              |            format: int64
              |            description: The Pets ID
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("Pet"), false)
        fields should be (Seq(
            Field("name", StringType, false, description = Some("The Pets name")),
            Field("tag", StringType),
            Field("id", LongType, false, description = Some("The Pets ID"), format=Some("int64"))
        ))
    }

    it should "support allOf for nested properties" in {
        val spec =
            """
              |swagger: "2.0"
              |info:
              |  version: 1.0.0
              |  title: Swagger Petstore
              |  description: A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification
              |  termsOfService: http://swagger.io/terms/
              |  contact:
              |    name: Swagger API Team
              |    email: apiteam@swagger.io
              |    url: http://swagger.io
              |  license:
              |    name: Apache 2.0
              |url: https://www.apache.org/licenses/LICENSE-2.0.html
              |definitions:
              |  Pet:
              |    properties:
              |      info:
              |        allOf:
              |          -
              |            type: object
              |            required:
              |              - name
              |            properties:
              |              name:
              |                type: string
              |                description: The Pets name
              |              tag:
              |                type: string
              |          -
              |            type: object
              |            required:
              |              - id
              |            properties:
              |              id:
              |                type: integer
              |                format: int64
              |                description: The Pets ID
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("Pet"), false)
        fields should be (Seq(
            Field("info", StructType(Seq(
                Field("name", StringType, false, description = Some("The Pets name")),
                Field("tag", StringType),
                Field("id", LongType, false, description = Some("The Pets ID"), format=Some("int64"))
            )))
        ))
    }

    it should "support allOf in arrays" in {
        val spec =
            """
              |swagger: "2.0"
              |definitions:
              |  Address:
              |    description: "Service Provider's address"
              |    additionalProperties: false
              |    type: object
              |    properties:
              |      num:
              |        type: "integer"
              |        maximum: 255
              |        minimum: 0
              |      AddressLine:
              |        items:
              |          description: "One of multiple address lines"
              |          allOf:
              |            -
              |              type: "object"
              |              required:
              |                - "Latin"
              |              properties:
              |                Latin:
              |                  type: "string"
              |                  example: "John Doe/MR"
              |                LocalSpelling:
              |                  type: "string"
              |                  example: "李四/ MR"
              |            -
              |              type: "object"
              |              required:
              |                - "num"
              |              properties:
              |                num:
              |                  type: "integer"
              |                  maximum: 255
              |                  minimum: 0
              |        type: "array"
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("Address"), false)
        fields should be (Seq(
            Field("num", IntegerType),
            Field("AddressLine", ArrayType(
                StructType(Seq(
                    Field("Latin", StringType, false),
                    Field("LocalSpelling", StringType),
                    Field("num", IntegerType, false)
                ))
            ))
        ))
    }

    it should "support untyped enums" in {
        val spec =
            """
              |swagger: "2.0"
              |definitions:
              |  Address:
              |    description: "Service Provider's address"
              |    additionalProperties: false
              |    type: object
              |    properties:
              |      type:
              |        enum:
              |         - A
              |         - B
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("Address"), false)
        fields.size should be (1)

        fields(0).nullable should be (true)
        fields(0).name should be ("type")
        fields(0).ftype should be (StringType)
    }

    it should "support untyped enums in sub objects" in {
        val spec =
            """
              |swagger: "2.0"
              |definitions:
              |  Address:
              |    description: "Service Provider's address"
              |    type: object
              |    properties:
              |      data:
              |        type: object
              |        properties:
              |          some_enum:
              |            enum:
              |             - A
              |             - B
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("Address"), false)
        fields.size should be (1)

        fields should be (Seq(
            Field("data", StructType(Seq(
                Field("some_enum", StringType)
            )))
        ))
    }

    it should "support integers, floats, doubles and decimals" in {
        val spec =
            """
              |swagger: "2.0"
              |definitions:
              |  Types:
              |    properties:
              |      int32:
              |        type: integer
              |        format: int32
              |      int64:
              |        type: integer
              |        format: int64
              |      float:
              |        type: number
              |        format: float
              |      double:
              |        type: number
              |        format: double
              |      number:
              |        type: number
              |      preciseNumber:
              |        type: number
              |        multipleOf: 0.01
              |      decimal:
              |        type: number
              |        multipleOf: 0.01
              |        maximum: 10000000
              |      string:
              |        type: string
              |      char_n:
              |        type: string
              |        minLength: 10
              |        maxLength: 10
              |      varchar_n:
              |        type: string
              |        minLength: 10
              |        maxLength: 20
              |""".stripMargin

        val fields = SwaggerSchemaUtils.fromSwagger(spec, Some("Types"), false)
        fields.size should be (10)

        fields(0).nullable should be (true)
        fields(0).name should be ("int32")
        fields(0).ftype should be (IntegerType)

        fields(1).nullable should be (true)
        fields(1).name should be ("int64")
        fields(1).ftype should be (LongType)

        fields(2).nullable should be (true)
        fields(2).name should be ("float")
        fields(2).ftype should be (FloatType)

        fields(3).nullable should be (true)
        fields(3).name should be ("double")
        fields(3).ftype should be (DoubleType)

        fields(4).nullable should be (true)
        fields(4).name should be ("number")
        fields(4).ftype should be (DecimalType(38,0))

        fields(5).nullable should be (true)
        fields(5).name should be ("preciseNumber")
        fields(5).ftype should be (DecimalType(38,2))

        fields(6).nullable should be (true)
        fields(6).name should be ("decimal")
        fields(6).ftype should be (DecimalType(10,2))

        fields(7).nullable should be (true)
        fields(7).name should be ("string")
        fields(7).ftype should be (StringType)

        fields(8).nullable should be (true)
        fields(8).name should be ("char_n")
        fields(8).ftype should be (CharType(10))

        fields(9).nullable should be (true)
        fields(9).name should be ("varchar_n")
        fields(9).ftype should be (VarcharType(20))
    }
}
