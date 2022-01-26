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
                val sep = p.indexOf('=')
                if (sep > 0)
                    Some(splitSetting(p))
                else
                    None
            }
            .filter(p => p._1.nonEmpty)
            .toMap
    }

    def splitSettings(settings: Seq[String]) : Seq[(String,String)] = {
        settings.map(splitSetting)
    }
    def splitSetting(setting: String) : (String,String) = {
        val sep = setting.indexOf('=')
        val key = setting.take(sep).trim
        val value = setting.drop(sep + 1).trim.replaceAll("^\"|\"$","")
        (key, value)
    }
}
