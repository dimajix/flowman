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
import java.sql.Date
import java.time.LocalDate
import java.time.Month

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.annotation.TemplateObject
import com.dimajix.flowman.util.UtcTimestamp


object TemplatingTest {
    @TemplateObject(name="SomeObject")
    class SomeObject {
        def upper(s:String) : String = s.toUpperCase
    }
}


class TemplatingTest extends AnyFlatSpec with Matchers {
    private val engine = Velocity.newEngine()
    private val context = Velocity.newContext()

    private def evaluate(text:String) : String = {
        val output = new StringWriter()
        engine.evaluate(context, output, "test", text)
        output.toString
    }

    "Integers" should "be parseable" in {
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

    "Floats" should "be parseable" in {
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

    "Booleans" should "be parseable" in {
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

    "Timestamps" should "be parseable from strings" in {
        evaluate("$Timestamp.parse('2017-10-10T10:00:00')") should be ("2017-10-10T10:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00Z')") should be ("2017-10-10T10:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00+00')") should be ("2017-10-10T10:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00+0000')") should be ("2017-10-10T10:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00+01')") should be ("2017-10-10T09:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00+0100')") should be ("2017-10-10T09:00:00.0")
        evaluate("$Timestamp.parse('2017-10-10T10:00:00').toEpochSeconds()") should be ("1507629600")
    }

    they should "support using timestamps" in {
        context.put("ts", UtcTimestamp.parse("2017-10-10T10:00:00.0"))
        evaluate("$Timestamp.parse($ts)") should be ("2017-10-10T10:00:00.0")
        evaluate("$Timestamp.format($ts, 'yyyy/MM/dd')") should be ("2017/10/10")
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

    "LocalDate" should "be parseable" in {
        evaluate("$LocalDate.parse('2017-10-10')") should be ("2017-10-10")
    }

    it should "be formattable" in {
        context.put("date_str", "2017-10-10")
        context.put("date_date", Date.valueOf("2017-10-10"))
        context.put("date_ldate", LocalDate.parse("2017-10-10"))
        evaluate("$LocalDate.format('2017-10-10', 'yyyy/MM/dd')") should be ("2017/10/10")
        evaluate("$LocalDate.format('2017-10-10', 'yyyy/MM/dd')") should be ("2017/10/10")
        evaluate("$LocalDate.format($date_str, 'yyyy/MM/dd')") should be ("2017/10/10")
        evaluate("$LocalDate.format($date_date, 'yyyy/MM/dd')") should be ("2017/10/10")
        evaluate("$LocalDate.format($date_ldate, 'yyyy/MM/dd')") should be ("2017/10/10")
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
        evaluate("${System.getenv('PATH')}") should be (System.getenv("PATH"))
        evaluate("${System.getenv('NO_SUCH_ENV')}") should be ("")
        evaluate("${System.getenv('NO_SUCH_ENV', 'default')}") should be ("default")
        evaluate("$System.getenv('PATH')") should be (System.getenv("PATH"))
        evaluate("$System.getenv('NO_SUCH_ENV')") should be ("")
        evaluate("$System.getenv('NO_SUCH_ENV', 'default')") should be ("default")
        evaluate("$System.getenv('NO_SUCH_ENV', $System.getenv('PATH'))") should be (System.getenv("PATH"))
        evaluate("$System.getenv('NO_SUCH_ENV', $String.concat('lala/',$System.getenv('PATH')))") should be ("lala/" + System.getenv("PATH"))
    }

    "The Velocity system" should "support new classes via annotation" in {
        evaluate("${SomeObject.upper('Hello World')}") should be ("HELLO WORLD")
    }
}
