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

import com.dimajix.flowman.dsl.MappingGen
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.mapping.AssembleMapping


object Assemble {
    type Entry = AssembleMapping.Entry
    val Append = AssembleMapping.AppendEntry
    type Append = AssembleMapping.AppendEntry
    val Flatten = AssembleMapping.FlattenEntry
    type Flatten = AssembleMapping.FlattenEntry
    val Lift = AssembleMapping.LiftEntry
    type Lift = AssembleMapping.LiftEntry
    val Rename = AssembleMapping.RenameEntry
    type Rename = AssembleMapping.RenameEntry
    val Struct = AssembleMapping.StructEntry
    type Struct = AssembleMapping.StructEntry
    val Nest = AssembleMapping.NestEntry
    type Nest = AssembleMapping.NestEntry
    val Explode = AssembleMapping.ExplodeEntry
    type Explode = AssembleMapping.ExplodeEntry
}

case class Assemble(
    input : MappingOutputIdentifier,
    columns: Seq[Assemble.Entry],
    filter:Option[String] = None
) extends MappingGen {
    def apply(props: Mapping.Properties): AssembleMapping = {
        AssembleMapping(
            props,
            input,
            columns,
            filter
        )
    }
}
