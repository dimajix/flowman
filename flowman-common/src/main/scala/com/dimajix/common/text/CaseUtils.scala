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

import java.util.Locale


object CaseUtils {
    /**
      * Provides a generic algorithm that splits an identifier into its individual components.
      * Several heuristics are used to work with camelCase and snake_case, but some edge cases
      * may fail
      * @param str
      * @return
      */
    def splitGeneric(str:String) : Seq[String] = {
        case class State(
            words:Seq[String],
            current:String,
            lower:Boolean=false
        )
        def zero = State(Seq(), "")
        def collect(state:State, c:Char) : State  = {
            if (!c.isLetterOrDigit) {
                val word = state.current
                if (word.nonEmpty)
                    State(state.words :+ word, "")
                else
                    State(state.words, "")
            }
            else if (c.isUpper && state.lower) {
                val word = state.current
                if (word.nonEmpty)
                    State(state.words :+ word, c.toString)
                else
                    State(state.words, c.toString)
            }
            else {
                State(state.words, state.current + c, c.isLower)
            }
        }

        val result = str.foldLeft(zero)(collect)
        result.words :+ result.current
    }

    /**
      * Splits a camelCase identifier into its individual components
      * @param str
      * @return
      */
    def splitCamel(str:String) : Seq[String] = {
        case class State(
            words:Seq[String],
            current:String
        )
        def zero = State(Seq(), "")
        def collect(state:State, c:Char) : State  = {
            if (!c.isLetterOrDigit) {
                val word = state.current
                if (word.nonEmpty)
                    State(state.words :+ word, "")
                else
                    State(state.words, "")
            }
            else if (c.isUpper) {
                val word = state.current
                if (word.nonEmpty)
                    State(state.words :+ word, c.toString)
                else
                    State(state.words, c.toString)
            }
            else {
                State(state.words, state.current + c)
            }
        }

        val result = str.foldLeft(zero)(collect)
        result.words :+ result.current
    }

    /**
      * Splits a kebab-case identifier into its components. Empty components will be removed
      * @param str
      * @return
      */
    def splitKebab(str:String) : Seq[String] = {
        str.split('-').filter(_.nonEmpty)
    }

    /**
      * Splits a snake_case identifier into its components. Empty components will be removed
      * @param str
      * @return
      */
    def splitSnake(str:String) : Seq[String] = {
        str.split('_').filter(_.nonEmpty)
    }

    /**
      * Joins together multiple words using CamelCase
      * @param words
      * @param firstUpperCase
      * @return
      */
    def joinCamel(words:Seq[String], firstUpperCase:Boolean=false) : String = {
        def capitalize(str:String) : String = {
            if (str.isEmpty)
                ""
            else
                str.head.toUpper + str.tail.toLowerCase(Locale.ROOT)
        }

        if (words.isEmpty) {
            ""
        }
        else {
            val head = if (firstUpperCase) capitalize(words.head) else words.head.toLowerCase(Locale.ROOT)
            val tail = words.tail.map(capitalize)
            (head +: tail).mkString
        }
    }

    /**
      * Joins together multiple words using kebab-case
      * @param words
      * @param upperCase
      * @return
      */
    def joinKebab(words:Seq[String], upperCase:Boolean=false) : String = {
        joinChar(words, "-", upperCase)
    }

    /**
      * Joins together multiple words using snake_case
      * @param words
      * @param upperCase
      * @return
      */
    def joinSnake(words:Seq[String], upperCase:Boolean=false) : String = {
        joinChar(words, "_", upperCase)
    }

    def joinChar(words:Seq[String], separator:String, upperCase:Boolean=false) : String = {
        val caseWords = if (upperCase)
            words.map(_.toUpperCase(Locale.ROOT))
        else
            words.map(_.toLowerCase(Locale.ROOT))
        caseWords.mkString(separator)
    }
}
