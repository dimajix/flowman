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

package com.dimajix.flowman.spec.output

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.MappingIdentifier


abstract class BaseOutput extends Output {
    @JsonProperty(value = "enabled", required=false) private var _enabled:String = _
    @JsonProperty(value = "input", required=true) private var _input:String = _

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))

    override def enabled(implicit context:Context) : Boolean = if (_enabled != null) context.evaluate(_enabled).toBoolean else true

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[MappingIdentifier] = {
        val table = input
        if (table != null)
            Array(table)
        else
            Array()
    }
}
