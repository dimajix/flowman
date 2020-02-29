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

package com.dimajix.flowman.model

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class IdentifierTest extends FlatSpec with Matchers {
    "The TableIdentifier" should "be parsed correctly" in {
        MappingIdentifier.parse("lala") should be (new MappingIdentifier("lala", None))
        MappingIdentifier.parse("project/lala") should be (new MappingIdentifier("lala", Some("project")))
        MappingIdentifier.parse("p1/p2/lala") should be (new MappingIdentifier("lala", Some("p1/p2")))
    }

    it should "be stringified corectly" in {
        new MappingIdentifier("lala", None).toString should be ("lala")
        new MappingIdentifier("lala", Some("project")).toString should be ("project/lala")
        new MappingIdentifier("lala", Some("p1/p2")).toString should be ("p1/p2/lala")
    }

    it should "support null values" in {
        MappingIdentifier.parse(null) should be (MappingIdentifier.empty)
    }
}
