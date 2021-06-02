/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.common.text

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class CaseUtilsTest extends AnyFlatSpec with Matchers {
    "CaseUtils" should "support generic splitting" in {
        CaseUtils.splitGeneric("ThisIsATest") should be (Seq("This", "Is", "ATest"))
        CaseUtils.splitGeneric("thisIsATest") should be (Seq("this", "Is", "ATest"))
        CaseUtils.splitGeneric("this_Is_A_Test") should be (Seq("this", "Is", "A", "Test"))
        CaseUtils.splitGeneric("this_is_a_test") should be (Seq("this", "is", "a", "test"))
        CaseUtils.splitGeneric("this_is_a_teSt") should be (Seq("this", "is", "a", "te", "St"))
        CaseUtils.splitGeneric("ATestIsThis") should be (Seq("ATest", "Is", "This"))
        CaseUtils.splitGeneric("a_test_is_this") should be (Seq("a", "test", "is", "this"))
        CaseUtils.splitGeneric("A_TEST_IS_THIS") should be (Seq("A", "TEST", "IS", "THIS"))
    }

    they should "support camel splitting" in {
        CaseUtils.splitCamel("ThisIsATest") should be (Seq("This", "Is", "A", "Test"))
        CaseUtils.splitCamel("thisIsATest") should be (Seq("this", "Is", "A", "Test"))
        CaseUtils.splitCamel("this_Is_A_Test") should be (Seq("this", "Is", "A", "Test"))
        CaseUtils.splitCamel("this_is_a_test") should be (Seq("this", "is", "a", "test"))
        CaseUtils.splitCamel("this_is_a_teSt") should be (Seq("this", "is", "a", "te", "St"))
        CaseUtils.splitCamel("ATestIsThis") should be (Seq("A", "Test", "Is", "This"))
        CaseUtils.splitCamel("a_test_is_this") should be (Seq("a", "test", "is", "this"))
        CaseUtils.splitCamel("A_TEST_IS_THIS") should be (Seq("A", "T", "E", "S", "T", "I", "S", "T", "H", "I", "S"))
    }

    they should "support snake splitting" in {
        CaseUtils.splitSnake("ThisIsATest") should be (Seq("ThisIsATest"))
        CaseUtils.splitSnake("thisIsATest") should be (Seq("thisIsATest"))
        CaseUtils.splitSnake("this_Is_A_Test") should be (Seq("this", "Is", "A", "Test"))
        CaseUtils.splitSnake("this_is_a_test") should be (Seq("this", "is", "a", "test"))
        CaseUtils.splitSnake("this_is_a_teSt") should be (Seq("this", "is", "a", "teSt"))
        CaseUtils.splitSnake("ATestIsThis") should be (Seq("ATestIsThis"))
        CaseUtils.splitSnake("a_test_is_this") should be (Seq("a", "test", "is", "this"))
        CaseUtils.splitSnake("A_TEST_IS_THIS") should be (Seq("A", "TEST", "IS", "THIS"))
    }

    they should "support camel joining" in {
        CaseUtils.joinCamel(Seq("A","test","is","THIS")) should be ("aTestIsThis")
        CaseUtils.joinCamel(Seq("A","test","is","THIS"), true) should be ("ATestIsThis")
    }

    they should "support snake joining" in {
        CaseUtils.joinSnake(Seq("A","test","is","THIS")) should be ("a_test_is_this")
        CaseUtils.joinSnake(Seq("A","test","is","THIS"), true) should be ("A_TEST_IS_THIS")
    }
}
