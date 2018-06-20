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

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ObjectMapper


class SwaggerSchemaTest extends FlatSpec with Matchers  {
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

        val session = Session.builder().build()
        implicit val context = session.context

        val result = ObjectMapper.parse[Schema](spec)
        result shouldBe an[SwaggerSchema]
        result.description should be ("A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification")

        val fields = result.fields
        fields.size should be (3)

        fields(0).nullable should be (false)
        fields(0).name should be ("name")
        fields(0).description should be ("The Pets name")
        fields(0).ftype should be (StringType)

        fields(1).nullable should be (true)
        fields(1).name should be ("tag")
        fields(1).ftype should be (StringType)

        fields(2).nullable should be (false)
        fields(2).name should be ("id")
        fields(2).description should be ("The Pets ID")
        fields(2).ftype should be (LongType)
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

        val session = Session.builder().build()
        implicit val context = session.context

        val result = ObjectMapper.parse[Schema](spec)
        result shouldBe an[SwaggerSchema]
        result.description should be ("A sample API that uses a petstore as an example to demonstrate features in the swagger-2.0 specification")

        val fields = result.fields
        fields.size should be (3)

        fields(0).nullable should be (false)
        fields(0).name should be ("name")
        fields(0).description should be ("The Pets name")
        fields(0).ftype should be (StringType)

        fields(1).nullable should be (true)
        fields(1).name should be ("tag")
        fields(1).ftype should be (StringType)

        fields(2).nullable should be (false)
        fields(2).name should be ("id")
        fields(2).description should be ("The Pets ID")
        fields(2).ftype should be (LongType)
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

        val session = Session.builder().build()
        implicit val context = session.context

        val result = ObjectMapper.parse[Schema](spec)
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
    }
}
