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

package com.dimajix.flowman.dsl

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Relation


class RelationWrapperFunctions(wrapper:Wrapper[Relation, Relation.Properties]) {
    def label(kv:(String,String)) : RelationWrapper = new RelationWrapper {
        override def gen: Relation.Properties => Relation = wrapper.gen
        override def props: Context => Relation.Properties = ctx => {
            val props = wrapper.props(ctx)
            props.copy(labels = props.labels + kv)
        }
    }
    def description(desc:String) : RelationWrapper = new RelationWrapper {
        override def gen: Relation.Properties => Relation = wrapper.gen
        override def props: Context => Relation.Properties = ctx =>
            wrapper.props(ctx).copy(description = Some(desc))
    }
}

case class RelationGenHolder(r:RelationGen) extends RelationWrapper {
    override def gen: Relation.Properties => Relation = r
    override def props: Context => Relation.Properties = c => Relation.Properties(c)
}
