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

import org.apache.velocity.exception.MethodInvocationException
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class ContextTest extends FlatSpec with Matchers {
    "Evaluation" should "work (1)" in {
        val context = RootContext.builder()
                .withEnvironment(Map("env_1" -> "value_1"), SettingLevel.NONE)
                .build()
        context.evaluate("$env_1") should be ("value_1")
    }

    it should "work (2)" in {
        val context = RootContext.builder()
            .withEnvironment(Map(("env_1", "value_1")), SettingLevel.NONE)
            .withEnvironment(Map(("env_2", "$env_1")), SettingLevel.NONE)
            .withEnvironment(Map(("env_3", "$env_2")), SettingLevel.NONE)
            .build()
        context.evaluate("$env_2") should be ("value_1")
        context.evaluate("$env_3") should be ("value_1")
    }

    it should "throw an exception on unknown vars" in {
        val context1 = RootContext.builder()
            .withEnvironment(Map(("env_1", "value_1")), SettingLevel.NONE)
            .build()
        a[MethodInvocationException] shouldBe thrownBy(context1.evaluate("$env_2"))

        val context2 = RootContext.builder()
            .withEnvironment(Map(("env_1", "value_1")), SettingLevel.NONE)
            .withEnvironment(Map(("env_2", "$env_1")), SettingLevel.NONE)
            .build()
        context2.evaluate("$env_2") should be ("value_1")
    }

    it should "support arithmetic operations" in {
        val context = RootContext.builder()
            .withEnvironment(Map(("a", "3")), SettingLevel.NONE)
            .withEnvironment(Map(("b", "2")), SettingLevel.NONE)
            .build()
        context.evaluate("#set($r=$Integer.parse($a)+$Integer.parse($b))$r") should be ("5")
        context.evaluate("$r") should be ("5")
    }

    it should "support any types" in {
        val context = RootContext.builder()
            .withEnvironment(Map(("a", 3)), SettingLevel.NONE)
            .withEnvironment(Map(("b", 2)), SettingLevel.NONE)
            .build()
        context.evaluate("#set($r=$a+$b)$r") should be ("5")
        context.evaluate("$r") should be ("5")
    }

    it should "support boolean values" in {
        val context = RootContext.builder()
            .withEnvironment(Map(("true_val", true)), SettingLevel.NONE)
            .withEnvironment(Map(("true_str", "true")), SettingLevel.NONE)
            .withEnvironment(Map(("false_val", false)), SettingLevel.NONE)
            .withEnvironment(Map(("false_str", "false")), SettingLevel.NONE)
            .build()
        context.evaluate("#if(true)1#{else}2#end") should be ("1")
        context.evaluate("#if(false)1#{else}2#end") should be ("2")
        context.evaluate("#if(${no_val})1#{else}2#end") should be ("2")
        context.evaluate("#if(${true_val})1#{else}2#end") should be ("1")
        context.evaluate("#if(${false_val})1#{else}2#end") should be ("2")
        context.evaluate("#if(${true_str})1#{else}2#end") should be ("1")
        context.evaluate("#if(${false_str})1#{else}2#end") should be ("1")
    }
}
