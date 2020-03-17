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
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spec.mapping.ReadRelationMapping
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue


case class Read(
    relation:Environment => RelationIdentifier,
    columns:Environment => Seq[Field] = _ => Seq(),
    partitions:Environment => Map[String,FieldValue] = _ => Map(),
    filter:Environment => Option[String] = _ => None
) extends MappingGen {
    def apply(props:Mapping.Properties) : ReadRelationMapping = {
        val env = props.context.environment
        ReadRelationMapping(
            props,
            relation(env),
            columns(env),
            partitions(env),
            filter(env)
        )
    }
}
