/*
 * Copyright 2018 Kaya Kupferschmidt
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Module
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.annotation.TargetType


case class AnnotatedTarget(instanceProperties:Target.Properties) extends BaseTarget {
}

@TargetType(kind = "annotatedTask")
class AnnotatedTargetSpec extends TargetSpec {
    override def instantiate(context: Context): Target = AnnotatedTarget(instanceProperties(context))
}



class PluginTargetTest extends AnyFlatSpec with Matchers  {
    "A plugin" should "be used if present" in {
        val session = Session.builder().build()
        val spec =
            """
              |targets:
              |  custom:
              |    kind: annotatedTask
            """.stripMargin
        val module = Module.read.string(spec)
        module.targets.keys should contain("custom")
        val target = module.targets("custom").instantiate(session.context)
        target shouldBe an[AnnotatedTarget]
    }

}
