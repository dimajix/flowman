/*
 * Copyright 2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.common

object ParserUtils {
    def parseDelimitedList(list:String) : Seq[String] = {
        list.split(',').map(_.trim).filter(_.nonEmpty)
    }

    def parseDelimitedKeyValues(list:String) : Map[String,String] = {
        list.split(',')
            .map(_.trim)
            .flatMap{ p =>
                val parts = p.split('=')
                if (parts.size == 2)
                    Some((parts(0),parts(1)))
                else
                    None
            }
            .filter(p => p._1.nonEmpty && p._2.nonEmpty)
            .toMap
    }
}
