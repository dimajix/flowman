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

package com.dimajix.flowman.templating

import java.io.StringWriter

import org.apache.velocity.exception.MethodInvocationException
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class VelocityTest extends FlatSpec with Matchers {
    private val engine = Velocity.newEngine()
    private val context = Velocity.newContext()

    private def evaluate(text:String) : String = {
        val output = new StringWriter()
        engine.evaluate(context, output, "test", text)
        output.toString
    }

    "The Velocity Engine" should "throw an exception on unknown references" in {
        a[MethodInvocationException] shouldBe thrownBy(evaluate("$unknown"))
    }

    it should "support escaping" in {
        val result = evaluate("\\$unknown")
        result should be ("$unknown")
    }

    it should "support boolean values" in {
        context.put("true_val", true)
        context.put("false_val", false)
        evaluate("#if(true)1#{else}2#end") should be ("1")
        evaluate("#if(false)1#{else}2#end") should be ("2")
        evaluate("#if(${true_val})1#{else}2#end") should be ("1")
        evaluate("#if(${false_val})1#{else}2#end") should be ("2")
        evaluate("true") should be ("true")
        evaluate("false") should be ("false")
        evaluate("$true_val") should be ("true")
        evaluate("$false_val") should be ("false")
    }

    it should "support boolean arithmetics" in {
        context.put("true_val", true)
        context.put("false_val", false)
        evaluate("#set($r=$true_val && $true_val)$r") should be ("true")
        evaluate("#set($r=$false_val && $true_val)$r") should be ("false")
        evaluate("#set($r=$false_val || $true_val)$r") should be ("true")
    }

    it should "support float values" in {
        context.put("f", 2.0)
        evaluate("$f") should be ("2.0")

        val three = 3.2
        context.put("three", three)
        evaluate("$three") should be ("3.2")
    }

    it should "supported integer values" in {
        context.put("int", 2)
        evaluate("$int") should be ("2")

        val three = 3
        context.put("three", three)
        evaluate("$three") should be ("3")
    }

    it should "support integer arithmetics" in {
        context.put("a", 2)
        context.put("b", 3)
        evaluate("#set($r=$a+$b)$r") should be ("5")
    }

    it should "support recursive evaluation via RecursiveValue" in {
        context.put("a", RecursiveValue(engine, context, "This is $b"))
        context.put("b", RecursiveValue(engine, context, "$c"))
        context.put("c", RecursiveValue(engine, context, "ccc"))
        evaluate("a=$a") should be ("a=This is ccc")
    }
}
