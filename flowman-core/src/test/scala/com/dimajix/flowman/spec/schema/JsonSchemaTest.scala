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
import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.DoubleType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType

class JsonSchemaTest extends FlatSpec with Matchers {
    "An JsonSchema" should "be declarable inline" in {
        val spec =
            """
              |kind: json
              |spec: |
              |  {
              |    "description": "Total amount for this Fee",
              |    "javaType": "com.dimajix.json.AmountWithIndicators",
              |    "additionalProperties": false,
              |    "patternProperties": {
              |      "^\\w+$": {}
              |    },
              |    "properties": {
              |      "AmountValue": {
              |        "javaType": "com.dimajix.json.Amount",
              |        "description": "Amount, includes Sign and Decimal Places from formats, plus Currency Code as an attribute",
              |        "required": [
              |          "value",
              |          "currencyCode"
              |        ],
              |        "type": "object",
              |        "properties": {
              |          "currencyCode": {
              |            "description": "ISO-4217 Currency Code (AN3)",
              |            "pattern": "^([A-Z]{3,3})$",
              |            "type": "string"
              |          },
              |          "value": {
              |            "description": "Value part of an amount. Leading digits: 14; fraction digits: 2. Max. Value 9999999999999.99",
              |            "type": "number"
              |          }
              |        }
              |      },
              |      "NetGrossIndicator": {
              |        "additionalProperties": false,
              |        "description": "Indicates whether this amount is net or gross",
              |        "javaType": "com.dimajix.json.NetGrossIndicator",
              |        "enum": [
              |          "Net",
              |          "Gross"
              |        ],
              |        "type": "string"
              |      }
              |    },
              |    "required": [
              |      "AmountValue"
              |    ],
              |    "type": "object"
              |  }
              |""".stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = ObjectMapper.parse[Schema](spec)
        result shouldBe an[JsonSchema]
        result.description should be ("Total amount for this Fee")

        val fields = result.fields
        fields.size should be (2)
        fields(0).nullable should be (false)
        fields(0).name should be ("AmountValue")
        fields(0).description should be ("Amount, includes Sign and Decimal Places from formats, plus Currency Code as an attribute")
        fields(0).ftype shouldBe a[StructType]

        fields(1).nullable should be (true)
        fields(1).name should be ("NetGrossIndicator")
        fields(1).description should be ("Indicates whether this amount is net or gross")
        fields(1).ftype should be (StringType)
    }

    it should "support arrays" in {
        val spec =
            """
              |kind: json
              |spec: |
              |  {
              |    "description": "Total amount for this Fee",
              |    "javaType": "com.dimajix.json.AmountWithIndicators",
              |    "additionalProperties": false,
              |    "patternProperties": {
              |      "^\\w+$": {}
              |    },
              |    "properties": {
              |      "AmountValue": {
              |        "javaType": "com.dimajix.json.Amount",
              |        "description": "Number Array",
              |        "required": [
              |          "value"
              |        ],
              |        "type": "array",
              |        "items": {
              |          "type": "number"
              |        }
              |      },
              |      "NetGrossIndicator": {
              |        "additionalProperties": false,
              |        "description": "Indicates whether this amount is net or gross",
              |        "javaType": "com.dimajix.json.NetGrossIndicator",
              |        "enum": [
              |          "Net",
              |          "Gross"
              |        ],
              |        "type": "string"
              |      }
              |    },
              |    "required": [
              |      "AmountValue"
              |    ],
              |    "type": "object"
              |  }
              |""".stripMargin

        val session = Session.builder().build()
        implicit val context = session.context

        val result = ObjectMapper.parse[Schema](spec)
        result shouldBe an[JsonSchema]
        result.description should be ("Total amount for this Fee")

        val fields = result.fields
        fields.size should be (2)
        fields(0).nullable should be (false)
        fields(0).name should be ("AmountValue")
        fields(0).description should be ("Number Array")
        fields(0).ftype should be (ArrayType(DoubleType))

        fields(1).nullable should be (true)
        fields(1).name should be ("NetGrossIndicator")
        fields(1).description should be ("Indicates whether this amount is net or gross")
        fields(1).ftype should be (StringType)
    }
}
