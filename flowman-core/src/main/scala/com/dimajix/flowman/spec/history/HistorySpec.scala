/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.history

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.spi.TypeRegistry


object HistorySpec extends TypeRegistry[HistorySpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "null", value = classOf[NullHistorySpec]),
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcHistorySpec])
))
abstract class HistorySpec {
    def instantiate(context:Context): StateStore
}
