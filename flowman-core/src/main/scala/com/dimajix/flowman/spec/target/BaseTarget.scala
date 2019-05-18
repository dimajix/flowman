/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.state.TargetInstance


abstract class BaseTarget extends Target {
    protected override def instanceProperties : Target.Properties

    /**
      * Returns true if the output should be executed per default
      * @return
      */
    override def enabled : Boolean = instanceProperties.enabled

    /**
      * Returns an instance representing this target with the context
      * @return
      */
    override def instance : TargetInstance = {
        TargetInstance(
            Option(context.namespace).map(_.name).getOrElse(""),
            Option(context.project).map(_.name).getOrElse(""),
            name
        )
    }

    /**
      * Returns the dependencies of this taret, which is exactly one input mapping
      *
      * @return
      */
    override def dependencies : Array[MappingIdentifier] = {
        val mapping = instanceProperties.input
        if (mapping != null && mapping.nonEmpty)
            Array(mapping)
        else
            Array()
    }
}
