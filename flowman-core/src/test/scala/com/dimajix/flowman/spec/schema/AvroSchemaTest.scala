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
import com.dimajix.flowman.types.StringType


class AvroSchemaTest extends FlatSpec with Matchers {
    "An AvroSchema" should "be declarable inline" in {
        val spec =
            """
              |kind: avro
              |spec: |
              |    {
              |    "type": "record",
              |    "namespace": "",
              |    "name": "test_schema",
              |    "fields": [
              |        {
              |            "doc": "AccessDateTime as a string",
              |            "type": "string",
              |            "name": "AccessDateTime",
              |            "order": "ignore"
              |        },
              |        {
              |            "doc": "Event as a string",
              |            "type": "string",
              |            "name": "Event",
              |            "order": "ignore"
              |        }
              |    ]
              |    }
            """.stripMargin

        val session = Session.builder().build()
        val schemaSpec = ObjectMapper.parse[SchemaSpec](spec)
        schemaSpec shouldBe an[AvroSchemaSpec]

        val result = schemaSpec.instantiate(session.context)
        result shouldBe an[AvroSchema]

        val fields = result.fields
        fields.size should be (2)
        fields(0).nullable should be (false)
        fields(0).name should be ("AccessDateTime")
        fields(0).description should be ("AccessDateTime as a string")
        fields(0).ftype should be (StringType)

        fields(1).nullable should be (false)
        fields(1).name should be ("Event")
        fields(1).description should be ("Event as a string")
        fields(1).ftype should be (StringType)
    }

    it should "support nullable fields" in {
        val spec =
            """
              |kind: avro
              |spec: |
              |    {
              |    "type": "record",
              |    "namespace": "",
              |    "name": "test_schema",
              |    "doc": "Some Documentation",
              |    "fields": [
              |        {
              |            "doc": "AccessDateTime as a string",
              |            "type": ["string", "null"],
              |            "name": "AccessDateTime",
              |            "order": "ignore"
              |        }
              |    ]
              |    }
            """.stripMargin

        val session = Session.builder().build()
        val schemaSpec = ObjectMapper.parse[SchemaSpec](spec)
        schemaSpec shouldBe an[AvroSchemaSpec]

        val result = schemaSpec.instantiate(session.context)
        result shouldBe an[AvroSchema]
        result.description should be ("Some Documentation")

        val fields = result.fields
        fields.size should be (1)
        fields(0).nullable should be (true)
        fields(0).name should be ("AccessDateTime")
        fields(0).description should be ("AccessDateTime as a string")
        fields(0).ftype should be (StringType)
    }

    it should "be readable from an external file" in {
        val spec =
            """
              |kind: avro
              |file: test/schema/AvroSchema.json
            """.stripMargin

        val session = Session.builder().build()
        val schemaSpec = ObjectMapper.parse[SchemaSpec](spec)
        schemaSpec shouldBe an[AvroSchemaSpec]

        val result = schemaSpec.instantiate(session.context)
        result shouldBe an[AvroSchema]

        val fields = result.fields
        fields.size should be (1)
        fields(0).nullable should be (true)
        fields(0).name should be ("AccessDateTime")
        fields(0).description should be ("AccessDateTime as a string")
        fields(0).ftype should be (StringType)
    }
}
