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

import com.dimajix.flowman.LocalSparkSession


class ContextTest extends FlatSpec with Matchers {
    "Evaluation" should "work (1)" in {
        val context = RootContext.builder()
                .withEnvironment(Seq(("env_1", "value_1")), SettingLevel.NONE)
                .build()
        context.evaluate("$env_1") should be ("value_1")
    }

    it should "work (2)" in {
        val context = RootContext.builder()
            .withEnvironment(Seq(("env_1", "value_1")), SettingLevel.NONE)
            .withEnvironment(Seq(("env_2", "$env_1")), SettingLevel.NONE)
            .withEnvironment(Seq(("env_3", "$env_2")), SettingLevel.NONE)
            .build()
        context.evaluate("$env_2") should be ("value_1")
        context.evaluate("$env_3") should be ("value_1")
    }

    it should "not replace unknown vars" in {
        val context1 = RootContext.builder()
            .withEnvironment(Seq(("env_1", "value_1")), SettingLevel.NONE)
            .build()
        context1.evaluate("$env_2") should be ("$env_2")

        val context2 = RootContext.builder()
            .withEnvironment(Seq(("env_1", "value_1")), SettingLevel.NONE)
            .withEnvironment(Seq(("env_2", "$env_1")), SettingLevel.NONE)
            .build()
        context2.evaluate("$env_2") should be ("value_1")
    }

    it should "support arithmetic operations" in {
        val context = RootContext.builder()
            .withEnvironment(Seq(("a", "3")), SettingLevel.NONE)
            .withEnvironment(Seq(("b", "2")), SettingLevel.NONE)
            .build()
        context.evaluate("#set($r=$Integer.parse($a)+$Integer.parse($b))$r") should be ("5")
        context.evaluate("$r") should be ("5")
    }

    it should "support any types" in {
        val context = RootContext.builder()
            .withEnvironment(Seq(("a", 3)), SettingLevel.NONE)
            .withEnvironment(Seq(("b", 2)), SettingLevel.NONE)
            .build()
        context.evaluate("#set($r=$a+$b)$r") should be ("5")
        context.evaluate("$r") should be ("5")
    }
}
