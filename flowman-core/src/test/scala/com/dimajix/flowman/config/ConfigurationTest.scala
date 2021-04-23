/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigurationTest extends AnyFlatSpec with Matchers {
    "The Configuration" should "apply all configs" in {
        val config = new Configuration(Map(
            "spark.lala" -> "spark_lala",
            "flowman.lolo" -> "flowman_lolo",
            "other.abc" -> "other_abc"
            ))

        config.get("spark.lala") should be ("spark_lala")
        config.get("flowman.lolo") should be ("flowman_lolo")
        config.get("other.abc") should be ("other_abc")

        config.sparkConf.get("spark.lala") should be ("spark_lala")
        config.sparkConf.contains("flowman.lolo") should be (false)
        config.sparkConf.get("other.abc") should be ("other_abc")

        config.flowmanConf.get("flowman.lolo") should be ("flowman_lolo")
        config.flowmanConf.contains("spark.lala") should be (false)
        config.flowmanConf.contains("other.abc") should be (false)
    }
}
