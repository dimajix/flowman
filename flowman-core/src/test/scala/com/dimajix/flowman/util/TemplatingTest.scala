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
import java.time.Month

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.annotation.TemplateObject
import com.dimajix.flowman.spi.Registration


object TemplatingTest {
    @TemplateObject(name="SomeObject")
    class SomeObject {
        def upper(s:String) : String = s.toUpperCase
    }
}


class TemplatingTest extends FlatSpec with Matchers {
    Registration.load()
    private val engine = Templating.newEngine()
    private val context = Templating.newContext()

    private def evaluate(text:String) : String = {
        val output = new StringWriter()
        engine.evaluate(context, output, "test", text)
        output.toString
    }

    "Integers" should "be supported as values" in {
        context.put("int", 2)
        val output = evaluate("$int")
        output.toString should be ("2")

        val three = 3
        context.put("three", three)
        val output2 = evaluate("$three")
        output2.toString should be ("3")
    }

    they should "support arithmetics" in {
        context.put("a", 2)
        context.put("b", 3)
        val output = new StringWriter()
        engine.evaluate(context, output, "test", "#set($r=$a+$b)$r")
        output.toString should be ("5")
    }

    they should "be parseable" in {
        context.put("one_val", 1)
        context.put("two_val", 2)
        context.put("one_str", "1")
        context.put("two_str", "2")
        evaluate("$Integer.parse(1)") should be ("1")
        evaluate("$Integer.parse(2)") should be ("2")
        evaluate("$Integer.parse('1')") should be ("1")
        evaluate("$Integer.parse('2')") should be ("2")
        evaluate("$Integer.parse($one_val)") should be ("1")
        evaluate("$Integer.parse($two_val)") should be ("2")
        evaluate("$Integer.parse($one_str)") should be ("1")
        evaluate("$Integer.parse($two_str)") should be ("2")
    }


    "Floats" should "be supported as values" in {
        context.put("f", 2.0)
        val output = new StringWriter()
        engine.evaluate(context, output, "test", "$f")
        output.toString should be ("2.0")

        val three = 3.2
        context.put("three", three)
        val output2 = new StringWriter()
        engine.evaluate(context, output2, "test", "$three")
        output2.toString should be ("3.2")
    }

    they should "be parseable" in {
        context.put("one_int", 1)
        context.put("two_int", 2)
        context.put("one_float", 1.0f)
        context.put("two_float", 2.0f)
        context.put("one_dbl", 1.2)
        context.put("two_dbl", 2.2)
        context.put("one_str", "1.2")
        context.put("two_str", "2.2")
        evaluate("$Float.parse(1)") should be ("1.0")
        evaluate("$Float.parse(2)") should be ("2.0")
        evaluate("$Float.parse(1.2)") should be ("1.2")
        evaluate("$Float.parse(2.2)") should be ("2.2")
        evaluate("$Float.parse('1')") should be ("1.0")
        evaluate("$Float.parse('2')") should be ("2.0")
        evaluate("$Float.parse($one_int)") should be ("1.0")
        evaluate("$Float.parse($two_int)") should be ("2.0")
        evaluate("$Float.parse($one_float)") should be ("1.0")
        evaluate("$Float.parse($two_float)") should be ("2.0")
        evaluate("$Float.parse($one_dbl)") should be ("1.2")
        evaluate("$Float.parse($two_dbl)") should be ("2.2")
        evaluate("$Float.parse($one_str)") should be ("1.2")
        evaluate("$Float.parse($two_str)") should be ("2.2")
    }

    they should "support arithmetics" in {
        context.put("a", 2.0)
        context.put("b", 3.0)
        val output = new StringWriter()
        engine.evaluate(context, output, "test", "#set($r=$a+$b)$r")
        output.toString should be ("5.0")
    }

    "Booleans" should "be supported as values" in {
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

    they should "be parseable" in {
        context.put("true_val", true)
        context.put("false_val", false)
        context.put("true_str", "true")
        context.put("false_str", "false")
        evaluate("$Boolean.parse(true)") should be ("true")
        evaluate("$Boolean.parse(false)") should be ("false")
        evaluate("$Boolean.parse('true')") should be ("true")
        evaluate("$Boolean.parse('false')") should be ("false")
        evaluate("$Boolean.parse($true_val)") should be ("true")
        evaluate("$Boolean.parse($false_val)") should be ("false")
        evaluate("$Boolean.parse($true_str)") should be ("true")
        evaluate("$Boolean.parse($false_str)") should be ("false")
    }

    they should "support arithmetics" in {
        context.put("true_val", true)
        context.put("false_val", false)
        evaluate("#set($r=$true_val && $true_val)$r") should be ("true")
        evaluate("#set($r=$false_val && $true_val)$r") should be ("false")
        evaluate("#set($r=$false_val || $true_val)$r") should be ("true")
    }

    "Timestamps" should "be supported" in {
        evaluate("$Timestamp.parse('2017-10-10T10:00:00')") should be ("2017-10-10T10:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00Z')") should be ("2017-10-10T10:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00+00')") should be ("2017-10-10T10:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00+0000')") should be ("2017-10-10T10:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00+01')") should be ("2017-10-10T09:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00+0100')") should be ("2017-10-10T09:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00').toEpochSeconds()") should be ("1507629600")
    }

    they should "be convertible to LocalDateTime" in {
        val output = new StringWriter()
        engine.evaluate(context, output, "test", "$Timestamp.parse('2017-10-10T10:11:23').toLocalDateTime()")
        output.toString should be ("2017-10-10T10:11:23")
    }

    they should "be convertible to epochs" in {
        evaluate("$Timestamp.parse('2017-10-10T10:11:23').toEpochSeconds()") should be ("1507630283")
        evaluate("$Timestamp.parse('2017-10-10T10:11:23Z').toEpochSeconds()") should be ("1507630283")
        evaluate("$Timestamp.parse('2017-10-10T10:11:23+00').toEpochSeconds()") should be ("1507630283")
        evaluate("$Timestamp.parse('2017-10-10T10:11:23+0000').toEpochSeconds()") should be ("1507630283")
    }

    they should "support complex operations" in {
        context.put("ts", UtcTimestamp.of(2017, Month.JUNE, 19, 0, 0))
        val output = new StringWriter()
        engine.evaluate(context, output, "test", """data/$ts.format("yyyy/MM/dd")/${ts.toEpochSeconds()}.i-*.log""")
        output.toString should be ("data/2017/06/19/1497830400.i-*.log")
    }

    "LocalDateTime" should "be parseable" in {
        evaluate("$LocalDateTime.parse('2017-10-10T10:00:00')") should be ("2017-10-10T10:00")
        evaluate("$LocalDateTime.parse('2017-10-10T10:00')") should be ("2017-10-10T10:00")
    }

    "Durations" should "be parseable" in {
        context.put("one_int", 1)
        context.put("two_int", 2)
        context.put("one_str", "1")
        context.put("two_str", "2")
        evaluate("$Duration.parse('P1D')") should be ("PT24H")
        evaluate("$Duration.ofDays(2)") should be ("PT48H")
        evaluate("$Duration.ofHours(7)") should be ("PT7H")
        evaluate("$Duration.ofDays($one_int)") should be ("PT24H")
        evaluate("$Duration.ofHours($two_int)") should be ("PT2H")
        evaluate("$Duration.ofDays($one_str)") should be ("PT24H")
        evaluate("$Duration.ofHours($two_str)") should be ("PT2H")
    }

    "Periods" should "be parseable" in {
        context.put("one_int", 1)
        context.put("two_int", 2)
        context.put("one_str", "1")
        context.put("two_str", "2")
        evaluate("$Period.parse('P1D')") should be ("P1D")
        evaluate("$Period.ofDays(2)") should be ("P2D")
        evaluate("$Period.ofWeeks(7)") should be ("P49D")
        evaluate("$Period.ofDays($one_int)") should be ("P1D")
        evaluate("$Period.ofWeeks($two_int)") should be ("P14D")
        evaluate("$Period.ofDays($one_str)") should be ("P1D")
        evaluate("$Period.ofWeeks($two_str)") should be ("P14D")
    }

    "String" should "provide concat functions" in {
        evaluate("$String.concat('abc','def')") should be ("abcdef")
        evaluate("$String.concat('abc','def', 'ghi')") should be ("abcdefghi")
        evaluate("$String.concat('abc','def', '1', '2')") should be ("abcdef12")
        evaluate("$String.concat('abc','def', '1', '2', '3')") should be ("abcdef123")
    }

    "System" should "provide access to some system variables" in {
        val output1 = new StringWriter()
        engine.evaluate(context, output1, "test", "${System.getenv('USER')}")
        output1.toString should be (System.getenv("USER"))

        val output2 = new StringWriter()
        engine.evaluate(context, output2, "test", "${System.getenv('NO_SUCH_ENV')}")
        output2.toString should be ("")

        val output3 = new StringWriter()
        engine.evaluate(context, output3, "test","${System.getenv('NO_SUCH_ENV', 'default')}")
        output3.toString should be ("default")

        val output4 = new StringWriter()
        engine.evaluate(context, output4, "test","$System.getenv('USER')")
        output4.toString should be (System.getenv("USER"))

        val output5 = new StringWriter()
        engine.evaluate(context, output5, "test","$System.getenv('NO_SUCH_ENV')")
        output5.toString should be ("")

        val output6 = new StringWriter()
        engine.evaluate(context, output6, "test","$System.getenv('NO_SUCH_ENV', 'default')")
        output6.toString should be ("default")

        val output7 = new StringWriter()
        engine.evaluate(context, output7, "test", "$System.getenv('NO_SUCH_ENV', $System.getenv('USER'))")
        output7.toString should be (System.getenv("USER"))

        val output8 = new StringWriter()
        engine.evaluate(context, output8, "test", """$System.getenv('NO_SUCH_ENV', $String.concat('lala/',$System.getenv('USER')))""")
        output8.toString should be ("lala/" + System.getenv("USER"))
    }

    "The Templating systeM" should "support new classes via annotation" in {
        evaluate("${SomeObject.upper('Hello World')}") should be ("HELLO WORLD")
    }
}
