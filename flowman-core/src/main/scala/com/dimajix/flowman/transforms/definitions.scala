/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.transforms

import java.util.Locale

import com.dimajix.common.text.CaseUtils
import com.dimajix.flowman.transforms.CaseFormat.CAMEL_CASE.name
import com.dimajix.flowman.transforms.CaseFormat.CAMEL_CASE.name
import com.dimajix.flowman.transforms.CaseFormat.CAMEL_CASE.name


sealed abstract class CaseFormat(val name:String) extends Product with Serializable {
    override def toString() : String = name
    def format(str:String) : String
    def join(words:String*) : String
    def concat(prefix:String,token:String) : String
}
object CaseFormat {
    case object CAMEL_CASE extends CaseFormat("camelCase") {
        override def format(str:String) : String = {
            val words = CaseUtils.splitGeneric(str)
            CaseUtils.joinCamel(words)
        }
        override def join(words:String*) : String = {
            CaseUtils.joinCamel(words)
        }
        override def concat(prefix:String,token:String) : String = {
            if (prefix.isEmpty) token else prefix + (token.head.toUpper + token.tail)
        }
    }

    case object CAMEL_CASE_UPPER extends CaseFormat("camelCaseUpper"){
        override def format(str:String) : String = {
            val words = CaseUtils.splitGeneric(str)
            CaseUtils.joinCamel(words, true)
        }
        override def join(words:String*) : String = {
            CaseUtils.joinCamel(words, true)
        }
        override def concat(prefix:String,token:String) : String = {
            val uname = token.head.toUpper + token.tail
            if (prefix.isEmpty) uname else prefix + uname
        }
    }

    case object SNAKE_CASE extends CaseFormat("snakeCase"){
        override def format(str:String) : String = {
            val words = CaseUtils.splitGeneric(str)
            CaseUtils.joinSnake(words)
        }
        override def join(words:String*) : String = {
            CaseUtils.joinSnake(words)
        }
        override def concat(prefix:String,token:String) : String = {
            if (prefix.isEmpty) token else prefix + "_" + token
        }
    }

    case object SNAKE_CASE_UPPER extends CaseFormat("snakeCaseUpper"){
        override def format(str:String) : String = {
            val words = CaseUtils.splitGeneric(str)
            CaseUtils.joinSnake(words, true)
        }
        override def join(words:String*) : String = {
            CaseUtils.joinSnake(words, true)
        }
        override def concat(prefix:String,token:String) : String = {
            val uname = token.toUpperCase(Locale.ROOT)
            if (prefix.isEmpty) uname else prefix + "_" + uname
        }
    }


    val ALL_CASES = Seq(CAMEL_CASE, CAMEL_CASE_UPPER, SNAKE_CASE, SNAKE_CASE_UPPER)

    def ofString(name:String) : CaseFormat = {
        val caseFormat = CaseUtils.joinCamel(CaseUtils.splitGeneric(name))

        caseFormat match {
            case "camelCase" => CAMEL_CASE
            case "camelCaseUpper" => CAMEL_CASE_UPPER
            case "snakeCase" => SNAKE_CASE
            case "snakeCaseUpper" => SNAKE_CASE_UPPER
            case _ => throw new IllegalArgumentException(s"Case format '$name' not supported, please use one of ${ALL_CASES.mkString(",")}")
        }
    }
}
