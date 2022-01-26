/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.dsl

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier



class TargetWrapperFunctions(wrapper:Wrapper[Target, Target.Properties]) {
    def label(kv:(String,String)) : TargetWrapper = new TargetWrapper {
        override def gen: Target.Properties => Target = wrapper.gen
        override def props: Context => Target.Properties = ctx => {
            val props = wrapper.props(ctx)
            props.copy(metadata = props.metadata.copy(labels=props.metadata.labels + kv))
        }
    }
}

case class TargetGenHolder(r:TargetGen) extends TargetWrapper {
    override def gen: Target.Properties => Target = r
    override def props: Context => Target.Properties = c => Target.Properties(c)
}
