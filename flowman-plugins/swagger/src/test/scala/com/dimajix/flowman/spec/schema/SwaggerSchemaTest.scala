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

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.DecimalType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FloatType
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType


class SwaggerSchemaTest extends AnyFlatSpec with Matchers  {
    "A Swagger Schema" should "be deserializable" in {
        val spec =
            """
              |kind: swagger
              |entity: Pet
              |spec: |
              |    swagger: "2.0"
              |    info:
              |      version: 1.0.0
              |      title: Swagger Petstore
              |      description: A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification
              |      termsOfService: http://swagger.io/terms/
              |      contact:
              |        name: Swagger API Team
              |        email: apiteam@swagger.io
              |        url: http://swagger.io
              |      license:
              |        name: Apache 2.0
              |    url: https://www.apache.org/licenses/LICENSE-2.0.html
              |    definitions:
              |      Pet:
              |        required:
              |          - name
              |          - id
              |        properties:
              |          name:
              |            type: string
              |            description: The Pets name
              |          tag:
              |            type: string
              |          id:
              |            type: integer
              |            format: int64
              |            description: The Pets ID
              |""".stripMargin

        val session = Session.builder().disableSpark().build()
        val schemaSpec = ObjectMapper.parse[SchemaSpec](spec)
        schemaSpec shouldBe an[SwaggerSchemaSpec]

        val result = schemaSpec.instantiate(session.context)
        result shouldBe an[SwaggerSchema]
        result.description should be (Some("A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification"))

        val fields = result.fields
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

        session.shutdown()
    }

    it should "support additionalProperties: false" in {
        val spec =
            """
              |kind: swagger
              |entity: Pet
              |spec: |
              |    swagger: "2.0"
              |    info:
              |      version: 1.0.0
              |      title: Swagger Petstore
              |      description: A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification
              |      termsOfService: http://swagger.io/terms/
              |      contact:
              |        name: Swagger API Team
              |        email: apiteam@swagger.io
              |        url: http://swagger.io
              |      license:
              |        name: Apache 2.0
              |    url: https://www.apache.org/licenses/LICENSE-2.0.html
              |    definitions:
              |      Pet:
              |        required:
              |          - name
              |          - id
              |          - transactionData
              |        properties:
              |          name:
              |            type: string
              |            description: The Pets name
              |          tag:
              |            type: string
              |          id:
              |            type: integer
              |            format: int64
              |            description: The Pets ID
              |          transactionData:
              |            description: 'transaction type meta-information'
              |            type: object
              |            properties:
              |              flight:
              |                description: 'transaction type flight, if transactionType is set to Flight this might contain data'
              |                additionalProperties: false
              |                properties:
              |                  additionalDataOnlyIndicator:
              |                    description: 'Indicates things'
              |                    type: boolean
              |                  uid:
              |                    type: string
              |                    description: unique ID (UUID)
              |                    format: uuid
              |                    example: 105a76d8-db49-4144-ace7-e683e8f4ba46
              |""".stripMargin

        val session = Session.builder().disableSpark().build()
        val schemaSpec = ObjectMapper.parse[SchemaSpec](spec)
        schemaSpec shouldBe an[SwaggerSchemaSpec]

        val result = schemaSpec.instantiate(session.context)
        result shouldBe an[SwaggerSchema]
        result.description should be(Some("A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification"))

        result.fields should be (Seq(
            Field("name", StringType, nullable = false, description = Some("The Pets name")),
            Field("tag", StringType, nullable = true, description = None),
            Field("id", LongType, nullable = false, description = Some("The Pets ID"), format=Some("int64")),
            Field("transactionData", StructType(
                Seq(
                    Field("flight", StructType(
                        Seq(
                            Field("additionalDataOnlyIndicator", BooleanType, nullable = true, description = Some("Indicates things")),
                            Field("uid", StringType, nullable = true, format=Some("uuid"), description = Some("unique ID (UUID)"))
                        )),
                        nullable = true,
                        description = Some("transaction type flight, if transactionType is set to Flight this might contain data")
                    )
                )),
                nullable = false,
                description = Some("transaction type meta-information")
            )
        ))

        session.shutdown()
    }

    it should "support allOf" in {
        val spec =
            """
              |kind: swagger
              |entity: Pet
              |spec: |
              |    swagger: "2.0"
              |    info:
              |      version: 1.0.0
              |      title: Swagger Petstore
              |      description: A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification
              |      termsOfService: http://swagger.io/terms/
              |      contact:
              |        name: Swagger API Team
              |        email: apiteam@swagger.io
              |        url: http://swagger.io
              |      license:
              |        name: Apache 2.0
              |    url: https://www.apache.org/licenses/LICENSE-2.0.html
              |    definitions:
              |      Pet:
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

        val session = Session.builder().disableSpark().build()
        val schemaSpec = ObjectMapper.parse[SchemaSpec](spec)
        schemaSpec shouldBe an[SwaggerSchemaSpec]

        val result = schemaSpec.instantiate(session.context)
        result shouldBe an[SwaggerSchema]
        result.description should be (Some("A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification"))

        val fields = result.fields
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

        session.shutdown()
    }

    it should "support nested allOf" in {
        val spec =
            """
              |kind: swagger
              |spec: |
              |    swagger: "2.0"
              |    definitions:
              |      Address:
              |        description: "Service Provider's address"
              |        additionalProperties: false
              |        type: object
              |        properties:
              |          num:
              |            type: "integer"
              |            maximum: 255
              |            minimum: 0
              |          AddressLine:
              |            items:
              |              description: "One of multiple address lines"
              |              allOf:
              |                -
              |                  type: "object"
              |                  required:
              |                    - "Latin"
              |                  properties:
              |                    Latin:
              |                      type: "string"
              |                      example: "John Doe/MR"
              |                    LocalSpelling:
              |                      type: "string"
              |                      example: "李四/ MR"
              |                -
              |                  type: "object"
              |                  required:
              |                    - "num"
              |                  properties:
              |                    num:
              |                      type: "integer"
              |                      maximum: 255
              |                      minimum: 0
              |            type: "array"
              |""".stripMargin

        val session = Session.builder().disableSpark().build()
        val schemaSpec = ObjectMapper.parse[SchemaSpec](spec)
        schemaSpec shouldBe an[SwaggerSchemaSpec]

        val result = schemaSpec.instantiate(session.context)
        result shouldBe an[SwaggerSchema]

        val fields = result.fields
        fields.size should be (2)

        fields(0).nullable should be (true)
        fields(0).name should be ("num")
        fields(0).ftype should be (IntegerType)

        fields(1).nullable should be (true)
        fields(1).name should be ("AddressLine")
        fields(1).ftype shouldBe an[ArrayType]

        val array = fields(1).ftype.asInstanceOf[ArrayType]
        array.containsNull should be (true)
        array.elementType shouldBe a[StructType]
        val struct = array.elementType.asInstanceOf[StructType]
        struct.fields.size should be (3)
        struct.fields(0).nullable should be (false)
        struct.fields(0).name should be ("Latin")
        struct.fields(0).ftype should be (StringType)

        struct.fields(1).nullable should be (true)
        struct.fields(1).name should be ("LocalSpelling")
        struct.fields(1).ftype should be (StringType)

        struct.fields(2).nullable should be (false)
        struct.fields(2).name should be ("num")
        struct.fields(2).ftype should be (IntegerType)

        session.shutdown()
    }

    it should "support integers, floats, doubles and decimals" in {
        val spec =
            """
              |kind: swagger
              |entity: Numbers
              |spec: |
              |    swagger: "2.0"
              |    definitions:
              |      Numbers:
              |        properties:
              |          int32:
              |            type: integer
              |            format: int32
              |          int64:
              |            type: integer
              |            format: int64
              |          float:
              |            type: number
              |            format: float
              |          double:
              |            type: number
              |            format: double
              |          number:
              |            type: number
              |          decimal:
              |            type: number
              |            multipleOf: 0.01
              |            maximum: 10000000
              |""".stripMargin

        val session = Session.builder().disableSpark().build()
        val schemaSpec = ObjectMapper.parse[SchemaSpec](spec)
        schemaSpec shouldBe an[SwaggerSchemaSpec]

        val result = schemaSpec.instantiate(session.context)
        result shouldBe an[SwaggerSchema]
        result.description should be (None)

        val fields = result.fields
        fields.size should be (6)

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
        fields(5).name should be ("decimal")
        fields(5).ftype should be (DecimalType(10,2))

        session.shutdown()
    }
}
