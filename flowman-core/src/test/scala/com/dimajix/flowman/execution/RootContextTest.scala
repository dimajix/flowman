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

package com.dimajix.flowman.execution

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class RootContextTest extends FlatSpec with Matchers {
    "The RootContext" should "provide access to some system variables" in {
        val session = Session.builder()
            .build()
        val context = session.context

        context.evaluate("${System.getenv('USER')}") should be (System.getenv("USER"))
        context.evaluate("${System.getenv('NO_SUCH_ENV')}") should be ("")
        context.evaluate("${System.getenv('NO_SUCH_ENV', 'default')}") should be ("default")

        context.evaluate("$System.getenv('USER')") should be (System.getenv("USER"))
        context.evaluate("$System.getenv('NO_SUCH_ENV')") should be ("")
        context.evaluate("$System.getenv('NO_SUCH_ENV', 'default')") should be ("default")
    }

    it should "provide access to Java Integer class" in {
        val session = Session.builder()
            .build()
        val context = session.context

        context.evaluate("${Integer.parse('2')}") should be ("2")
        context.evaluate("${Integer.valueOf('2')}") should be ("2")
    }

    it should "provide access to Java Float class" in {
        val session = Session.builder()
            .build()
        val context = session.context

        context.evaluate("${Float.parse('2')}") should be ("2.0")
        context.evaluate("${Float.valueOf('2')}") should be ("2.0")
    }

    it should "provide access to Java Duration class" in {
        val session = Session.builder()
            .build()
        val context = session.context

        context.evaluate("${Duration.ofDays(2)}") should be ("PT48H")
        context.evaluate("${Duration.parse('P2D').getSeconds()}") should be ("172800")
    }
}
