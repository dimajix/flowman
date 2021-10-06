/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.dsl.mapping

import com.dimajix.flowman.dsl.MappingGen
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.mapping.ExtractJsonMapping


case class ExtractJson(
    input:MappingOutputIdentifier,
    column:String,
    schema:Prototype[Schema],
    parseMode:String = "PERMISSIVE"
) extends MappingGen {
    def apply(props:Mapping.Properties) : ExtractJsonMapping = {
        ExtractJsonMapping(
            props,
            input,
            column,
            Some(schema.instantiate(props.context)),
            parseMode
        )
    }
}
