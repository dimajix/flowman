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

package com.dimajix.flowman.spec.schema

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.types._


/**
  * A PartitionField is a special field used for specifying partition columns. In addition to normal fields,
  * it also supports interpolation, but it lacks the capability of being nullable.
  */
class PartitionFieldSpec {
    @JsonProperty(value="name", required = true) private var name: String = _
    @JsonProperty(value="type", required = false) private var ftype: FieldType = _
    @JsonProperty(value="description", required = false) private var description: Option[String] = None
    @JsonProperty(value="granularity", required = false) private var granularity: Option[String] = None

    def instantiate(context: Context) : PartitionField = {
        PartitionField(
            context.evaluate(name),
            ftype,
            description.map(context.evaluate),
            granularity.map(context.evaluate)
        )
    }
}
