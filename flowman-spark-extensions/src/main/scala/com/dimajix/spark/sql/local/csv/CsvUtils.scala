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

package com.dimajix.spark.sql.local.csv

object CsvUtils {
    /**
      * Helper method that converts string representation of a character to actual character.
      * It handles some Java escaped strings and throws exception if given string is longer than one
      * character.
      */
    @throws[IllegalArgumentException]
    def toChar(str: String): Char = {
        if (str.charAt(0) == '\\') {
            str.charAt(1)
            match {
                case 't' => '\t'
                case 'r' => '\r'
                case 'b' => '\b'
                case 'f' => '\f'
                case '\"' => '\"' // In case user changes quote char and uses \" as delimiter in options
                case '\'' => '\''
                case '\\' => '\\'
                case 'u' if str == """\u0000""" => '\u0000'
                case _ =>
                    throw new IllegalArgumentException(s"Unsupported special character for delimiter: $str")
            }
        } else if (str.length == 1) {
            str.charAt(0)
        } else {
            throw new IllegalArgumentException(s"Delimiter cannot be more than one character: $str")
        }
    }

    /**
      * Filter ignorable rows for CSV iterator (lines empty and starting with `comment`).
      * This is currently being used in CSV reading path and CSV schema inference.
      */
    def filterCommentAndEmpty(iter: Iterator[String], options: CsvOptions): Iterator[String] = {
        iter.filter { line =>
            line.trim.nonEmpty && !line.startsWith(options.comment.toString)
        }
    }

    def skipComments(iter: Iterator[String], options: CsvOptions) : Iterator[String] = {
        if (options.isCommentSet) {
            val commentPrefix = options.comment.toString
            iter.dropWhile { line =>
                line.trim.isEmpty || line.trim.startsWith(commentPrefix)
            }
        } else {
            iter.dropWhile(_.trim.isEmpty)
        }
    }

    /**
      * Drop header line so that only data can remain.
      * This is similar with `filterHeaderLine` above and currently being used in CSV reading path.
      */
    def dropHeaderLine(iter: Iterator[String], options: CsvOptions): Iterator[String] = {
        skipComments(iter, options)
        if (iter.hasNext) iter.drop(1)
        iter
    }
}
