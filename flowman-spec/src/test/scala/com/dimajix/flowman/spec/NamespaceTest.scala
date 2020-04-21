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

package com.dimajix.flowman.spec

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.spec.history.NullHistorySpec


class NamespaceTest extends FlatSpec with Matchers {
    "A Namespace" should "be creatable from a spec" in {
        val spec =
            """
              |environment:
              | - lala=lolo
              |config:
              | - cfg1=cfg2=lala
              |history:
              |  kind: null
              |store:
              |  kind: null
            """.stripMargin
        val ns = Namespace.read.string(spec)
        ns.environment.size should be (1)
        ns.config.size should be (1)
    }

    it should "provide a default Namespace" in {
        val ns = Namespace.read.default()
        ns should not be (null)
        ns.name should be ("default")
        ns.history should be (None)
    }
}
