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

package com.dimajix.flowman.dsl.target

import com.dimajix.flowman.dsl.TargetGen
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.IdentifierRelationReference
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.target


case class RelationTarget(
    mapping:MappingOutputIdentifier = MappingOutputIdentifier.empty,
    relation:RelationIdentifier,
    mode:OutputMode = OutputMode.OVERWRITE,
    partition:Map[String,String] = Map(),
    parallelism:Int = 16,
    rebalance:Boolean = false
) extends TargetGen {
    override def apply(props:Target.Properties) : target.RelationTarget = {
        target.RelationTarget(
            props,
            mapping = mapping,
            relation = IdentifierRelationReference(props.context, relation),
            mode = mode,
            partition = partition,
            parallelism = parallelism,
            rebalance = rebalance
        )
    }
}
