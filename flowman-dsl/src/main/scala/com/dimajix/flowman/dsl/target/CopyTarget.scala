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
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.spec.target


object CopyTarget {
    val Schema = target.CopyTarget.Schema
    type Schema = target.CopyTarget.Schema
}
case class CopyTarget(
    source:Template[Dataset],
    target:Template[Dataset],
    schema:Option[CopyTarget.Schema] = None,
    parallelism:Int = 16,
    mode:OutputMode = OutputMode.OVERWRITE
) extends TargetGen {
    override def apply(props: Target.Properties): com.dimajix.flowman.spec.target.CopyTarget = {
        val context = props.context
        com.dimajix.flowman.spec.target.CopyTarget(
            props,
            source.instantiate(context),
            target.instantiate(context),
            schema,
            parallelism,
            mode
        )
    }
}
