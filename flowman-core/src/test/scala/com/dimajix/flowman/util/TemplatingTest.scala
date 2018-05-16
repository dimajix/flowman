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

package com.dimajix.flowman.util

import java.io.StringWriter

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class TemplatingTest extends FlatSpec with Matchers {
    private val engine = Templating.newEngine()
    private val context = Templating.newContext()

    "Templating" should "support Timestamps" in {
        val output = new StringWriter()
        engine.evaluate(context, output, "test", "$Timestamp.parse('2017-10-10 10:00:00')")
        output.toString should be ("2017-10-10 10:00:00.0")

        val output2 = new StringWriter()
        engine.evaluate(context, output2, "test", "$Timestamp.parse('2017-10-10 10:00:00').getTime()")
        output2.toString should be ("1507629600000")
    }
}
