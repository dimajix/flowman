/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.RootContext


class TargetTest extends AnyFlatSpec with Matchers {
    "Target.Properties" should "be copyable" in {
        val project = new Project(name = "project")
        val namespace = new Namespace(name = "default")
        val context = RootContext.builder(namespace)
            .build()

        val props = Target.Properties(context, name="t0", kind="some_kind")
        props.context should be (context)
        props.name should be ("t0")
        props.kind should be ("some_kind")
        props.namespace should be (Some(namespace))
        props.project should be (None)
        props.metadata.category should be (Category.TARGET.lower)
        props.metadata.kind should be ("some_kind")
        props.metadata.name should be ("t0")
        props.metadata.labels should be (Map())

        val props2 = props.copy(metadata = props.metadata.copy(name = "t1"))
        props2.context should be (context)
        props2.name should be ("t1")
        props2.kind should be ("some_kind")
        props2.namespace should be (Some(namespace))
        props2.project should be (None)
        props2.metadata.category should be (Category.TARGET.lower)
        props2.metadata.kind should be ("some_kind")
        props2.metadata.name should be ("t1")
        props2.metadata.labels should be (Map())

        val props3 = props.withName("t3")
        props3.context should be (context)
        props3.name should be ("t3")
        props3.kind should be ("some_kind")
        props3.namespace should be (Some(namespace))
        props3.project should be (None)
        props3.metadata.category should be (Category.TARGET.lower)
        props3.metadata.kind should be ("some_kind")
        props3.metadata.name should be ("t3")
        props3.metadata.labels should be (Map())
    }
}
