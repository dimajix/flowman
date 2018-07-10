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

package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier

class ShowMappingTask extends BaseTask {
    @JsonProperty(value="input", required=true) private var _input:String = _
    @JsonProperty(value="limit", required=true) private[spec] var _limit:String = "100"
    @JsonProperty(value="columns", required=true) private[spec] var _columns:Seq[String] = _

    def input(implicit context: Context) : MappingIdentifier = MappingIdentifier.parse(context.evaluate(_input))
    def limit(implicit context: Context) : Int = context.evaluate(_limit).toInt
    def columns(implicit context: Context) : Seq[String] = if (_columns != null) _columns.map(context.evaluate) else null

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val dfIn = executor.instantiate(this.input)
        val dfOut = if (_columns != null && _columns.nonEmpty)
            dfIn.select(columns.map(c => dfIn(c)):_*)
        else
            dfIn

        dfOut.show(limit)
        true
    }
}
