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

package com.dimajix.flowman.spec.mapping

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.fasterxml.jackson.annotation.JsonProperty


object CaseMappingSpec {
    class Case {
        @JsonProperty(value = "condition", required = true) private[CaseMappingSpec] var condition:String = ""
        @JsonProperty(value = "input", required = true) private[CaseMappingSpec] var input:String = ""
    }
}

class CaseMappingSpec extends MappingSpec {
    @JsonProperty(value = "cases", required = true) private var cases: Seq[CaseMappingSpec.Case] = Seq()

    /**
     * Creates the instance of the specified Mapping with all variable interpolation being performed
     * @param context
     * @return
     */
    override def instantiate(context: Context): AliasMapping = {
        def eval(cond:String) : Boolean = {
            context.evaluate(s"#if (${cond}) true #else false #end").trim.toBoolean
        }
        val props = instanceProperties(context)
        val input = cases.find(c => eval(c.condition))
        if (input.isEmpty)
            throw new IllegalArgumentException(s"No valid case found in case mapping '${props.identifier}'")

        AliasMapping(
            props,
            MappingOutputIdentifier(context.evaluate(input.get.input))
        )
    }
}
