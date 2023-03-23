/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.flowman.spec.relation

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.catalog.TableIndex
import com.dimajix.flowman.execution.Context


class IndexSpec {
    @JsonProperty(value = "name", required = true) protected var name: String = _
    @JsonProperty(value = "columns", required = true) protected var columns: Seq[String] = Seq.empty
    @JsonProperty(value = "unique", required = false) protected var unique: String = "false"
    @JsonProperty(value = "clustered", required = false) protected var clustered: String = "false"

    def instantiate(context:Context) : TableIndex = {
        TableIndex(
            context.evaluate(name),
            columns.map(context.evaluate),
            context.evaluate(unique).toBoolean,
            context.evaluate(clustered).toBoolean
        )
    }
}


trait IndexedRelationSpec { this: RelationSpec =>
    @JsonProperty(value = "indexes", required = false) protected var indexes: Seq[IndexSpec] = Seq.empty
}
