/*
 * Copyright (C) 2018 The Flowman Authors
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

import java.util.NoSuchElementException

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class EnvironmentTest extends AnyFlatSpec with Matchers {
    "The Environment" should "provide access to some system variables" in {
        val environment = new Environment(Map())

        environment.evaluate("${System.getenv('PATH')}") should be (System.getenv("PATH"))
        environment.evaluate("${System.getenv('NO_SUCH_ENV')}") should be ("")
        environment.evaluate("${System.getenv('NO_SUCH_ENV', 'default')}") should be ("default")

        environment.evaluate("$System.getenv('PATH')") should be (System.getenv("PATH"))
        environment.evaluate("$System.getenv('NO_SUCH_ENV')") should be ("")
        environment.evaluate("$System.getenv('NO_SUCH_ENV', 'default')") should be ("default")
    }

    it should "provide access to Java Integer class" in {
        val environment = new Environment(Map())

        environment.evaluate("${Integer.parse('2')}") should be ("2")
        environment.evaluate("${Integer.valueOf('2')}") should be ("2")
    }

    it should "provide access to Java Float class" in {
        val environment = new Environment(Map())

        environment.evaluate("${Float.parse('2')}") should be ("2.0")
        environment.evaluate("${Float.valueOf('2')}") should be ("2.0")
    }

    it should "provide access to Java Duration class" in {
        val environment = new Environment(Map())

        environment.evaluate("${Duration.ofDays(2)}") should be ("PT48H")
        environment.evaluate("${Duration.parse('P2D').getSeconds()}") should be ("172800")
    }

    it should "support evaluation of variables" in {
        val environment = new Environment(Map("var1" -> "val1"))

        environment.evaluate(null:String) should be (null)
        environment.evaluate("$var1") should be ("val1")
        environment.evaluate("$var1 + $var2", Map("var2" -> "val2")) should be ("val1 + val2")
        environment.evaluate("$var1", Map("var1" -> "val2")) should be ("val2")
    }

    it should "directly work with Options" in {
        val environment = new Environment(Map("var1" -> "val1"))

        environment.evaluate(Some("$var1")) should be (Some("val1"))
        environment.evaluate(None) should be (None)
        environment.evaluate(Some("$var1 + $var2"), Map("var2" -> "val2")) should be (Some("val1 + val2"))
        environment.evaluate(None, Map("var2" -> "val2")) should be (None)
    }

    it should "directly work with Maps" in {
        val environment = new Environment(Map("var1" -> "val1"))

        environment.evaluate(Map("a" -> "$var1", "b" -> "b")) should be(Map("a" -> "val1", "b" -> "b"))
        environment.evaluate(Map("a" -> "$var1", "b" -> "$var2"), Map("var2" -> "val2")) should be(Map("a" -> "val1", "b" -> "val2"))
    }

    it should "provide access to its key-value map" in {
        val environment = new Environment(Map("var1" -> "val1", "var2" -> "_${var1}_"))

        environment.toMap should be (Map("var1" -> "val1", "var2" -> "_val1_"))
        environment.toSeq.sortBy(_._1) should be (Seq("var1" -> "val1", "var2" -> "_val1_").sortBy(_._1))
        environment.keys should be (Set("var1", "var2"))
    }

    it should "provide map-like access" in {
        val environment = new Environment(Map("var1" -> "val1", "var2" -> "_${var1}_"))

        environment("var1") should be ("val1")
        environment("var2") should be ("_val1_")
        a[NoSuchElementException] should be thrownBy(environment("no_such_variable"))

        environment.get("var1") should be (Some("val1"))
        environment.get("var2") should be (Some("_val1_"))
        environment.get("no_such_variable") should be (None)
    }
}
