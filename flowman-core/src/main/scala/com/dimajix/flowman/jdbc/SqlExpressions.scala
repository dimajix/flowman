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

import com.dimajix.flowman.catalog.PartitionSpec


abstract class SqlExpressions {
    def in(column: String, values: Iterable[Any]): String

    def eq(column: String, value: Any): String

    def partition(partition: PartitionSpec): String

    def collate(charset:Option[String], collation:Option[String]) : String
}
