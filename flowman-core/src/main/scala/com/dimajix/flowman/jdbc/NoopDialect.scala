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

package com.dimajix.flowman.jdbc

class NoopDialect extends BaseDialect {
    /**
      * Check if this dialect instance can handle a certain jdbc url.
      *
      * @param url the jdbc url.
      * @return True if the dialect can be applied on the given jdbc url.
      * @throws NullPointerException if the url is null.
      */
    override def canHandle(url: String): Boolean = true
}
object NoopDialect extends NoopDialect
