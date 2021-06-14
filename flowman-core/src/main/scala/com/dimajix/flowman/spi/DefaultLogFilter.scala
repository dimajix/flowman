/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spi

import java.util.Locale

import com.dimajix.flowman.spi.DefaultLogFilter.redactedKeys


object DefaultLogFilter {
    val redactedKeys = Seq(
        "password$".r,
        "secret$".r,
        "credential$".r
    )
}

class DefaultLogFilter extends LogFilter {
    override def filterConfig(key: String, value: String): Option[(String, String)] = {
        val lwrKey = key.toLowerCase(Locale.ROOT)
        if (redactedKeys.exists(_.findFirstIn(lwrKey).nonEmpty)) {
            Some((key, "***redacted***"))
        }
        else {
            Some((key, value))
        }
    }
}
