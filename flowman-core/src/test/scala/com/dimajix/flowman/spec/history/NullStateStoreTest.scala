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

package com.dimajix.flowman.spec.history

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.spec.ObjectMapper


class NullStateStoreTest extends FlatSpec with Matchers {
    "The NullStateStoreSpec" should "be parseable" in {
        val spec =
            """
              |kind: null
            """.stripMargin

        val monitor = ObjectMapper.parse[HistorySpec](spec)
        monitor shouldBe a[NullHistorySpec]
    }

}
