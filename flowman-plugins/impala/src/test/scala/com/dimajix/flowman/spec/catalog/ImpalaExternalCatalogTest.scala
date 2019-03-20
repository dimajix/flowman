/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.catalog

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.catalog.ImpalaExternalCatalog
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Namespace


class ImpalaExternalCatalogTest extends FlatSpec with Matchers {
    "An ImpalaCatalog" should "be readable from a namespace definition" in {
        val spec =
            """
              |connections:
              |  impala:
              |    kind: jdbc
              |    url: jdbc:impala:localhost:21050
              |
              |catalog:
              |  kind: impala
              |  connection: impala
            """.stripMargin

        val namespace = Namespace.read.string(spec)
        namespace.catalog should not be (null)
        namespace.catalog shouldBe an[ImpalaCatalogProvider]

        val session = Session.builder()
            .withNamespace(namespace)
            .build()

        val catalog = namespace.catalog.createCatalog(session)
        catalog shouldBe an[ImpalaExternalCatalog]
    }
}
