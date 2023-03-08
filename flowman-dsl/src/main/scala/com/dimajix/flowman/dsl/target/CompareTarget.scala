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

package com.dimajix.flowman.dsl.target

import com.dimajix.flowman.dsl.TargetGen
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.target


case class CompareTarget(
    actual:Prototype[Dataset],
    expected:Prototype[Dataset]
) extends TargetGen {
    override def apply(props: Target.Properties): target.CompareTarget = {
        val context = props.context
        target.CompareTarget(
            props,
            actual.instantiate(context),
            expected.instantiate(context)
        )
    }
}
