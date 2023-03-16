/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.util.ReflectionUtilTest.CaseClass
import com.dimajix.util.ReflectionUtilTest.OtherStuff
import com.dimajix.util.ReflectionUtilTest.Stuff


object ReflectionUtilTest {
    class OtherStuff(val map:Map[String,String]) {

        def canEqual(other: Any): Boolean = other.isInstanceOf[OtherStuff]

        override def equals(other: Any): Boolean = other match {
            case that: OtherStuff =>
                (that canEqual this) &&
                    map == that.map
            case _ => false
        }

        override def hashCode(): Int = {
            val state = Seq(map)
            state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
        }
    }
    case class Stuff(other:OtherStuff) {

    }
    case class CaseClass(
        int:Int,
        str:String,
        seq:Seq[Int],
        map:Map[String,Boolean],
        stuff:Stuff
    )
}

class ReflectionUtilTest extends AnyFlatSpec with Matchers {
    "The Reflection" should "return a companion object" in {
        Reflection.companion("com.dimajix.util.Reflection") should be (Some(Reflection))
        Reflection.companion("com.dimajix.util.NoSuchObject") should be (None)
    }

    "Reflection.construct" should "work" in {
        Reflection.construct(classOf[CaseClass], Map("int" -> 12, "str" -> "lala")) should be (CaseClass(12, "lala", Seq.empty, Map.empty, Stuff(new OtherStuff(Map.empty))))
        Reflection.construct(classOf[CaseClass], Map("int" -> 12)) should be (CaseClass(12, "", Seq.empty, Map.empty, Stuff(new OtherStuff(Map.empty))))
    }
    it should "throw an exception for an unknown parameter" in {
        an[IllegalArgumentException] should be thrownBy(Reflection.construct(classOf[CaseClass], Map("int2" -> 12)))
    }
    it should "throw an exception for a missing fundamental type" in {
        an[IllegalArgumentException] should be thrownBy (Reflection.construct(classOf[CaseClass], Map("str" -> "lala")))
    }

    "Reflection.copy" should "work" in {
        val obj = CaseClass(12, "lala", Seq.empty, Map.empty, Stuff(new OtherStuff(Map.empty)))
        Reflection.copy(obj, Map("str" -> "lolo")) should be (CaseClass(12, "lolo", Seq.empty, Map.empty, Stuff(new OtherStuff(Map.empty))))
        Reflection.copy(obj, Map("int" -> 23)) should be (CaseClass(23, "lala", Seq.empty, Map.empty, Stuff(new OtherStuff(Map.empty))))
    }
    it should "throw an exception for an unknown parameter" in {
        val obj = CaseClass(12, "lala", Seq.empty, Map.empty, Stuff(new OtherStuff(Map.empty)))
        an[IllegalArgumentException] should be thrownBy(Reflection.copy(obj, Map("int2" -> 12)))
    }
}
