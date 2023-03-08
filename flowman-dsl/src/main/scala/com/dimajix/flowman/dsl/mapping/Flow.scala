/*
 * Copyright (C) 2018 The Flowman Authors
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

import com.dimajix.flowman.dsl.FieldMap
import com.dimajix.flowman.dsl.MappingGen
import com.dimajix.flowman.dsl.MappingList
import com.dimajix.flowman.dsl.Converters
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.spec.mapping.UnitMapping


class Flow extends MappingGen with Converters {
    val mappings : MappingList = new MappingList()
    val environment : FieldMap = new FieldMap()

    def apply(props:Mapping.Properties) : UnitMapping = {
        UnitMapping(
            props,
            mappings.toMap,
            environment.toMap
        )
    }
}
