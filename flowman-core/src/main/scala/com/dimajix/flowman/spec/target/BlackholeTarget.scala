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

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.ResourceIdentifier


case class BlackholeTarget(
    instanceProperties:Target.Properties,
    mapping:MappingOutputIdentifier
) extends BaseTarget {
    /**
      * Returns a list of physical resources produced by this target
      * @return
      */
    override def provides : Seq[ResourceIdentifier] = Seq()

    /**
      * Returns a list of physical resources required by this target
      * @return
      */
    override def requires : Seq[ResourceIdentifier] = MappingUtils.requires(context, mapping.mapping)

    override def create(executor: Executor) : Unit = {}

    override def migrate(executor: Executor) : Unit = {}

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override def build(executor:Executor) : Unit = {
        val mapping = context.getMapping(this.mapping.mapping)
        val df = executor.instantiate(mapping, this.mapping.output)
        df.write.format("null").save()
    }

    /**
      * "Cleaning" a blackhole essentially is a no-op
      * @param executor
      */
    override def truncate(executor: Executor): Unit = {}

    override def destroy(executor: Executor) : Unit = {}
}



class BlackholeTargetSpec extends TargetSpec {
    @JsonProperty(value = "input", required=true) private var input:String = _

    override def instantiate(context: Context): BlackholeTarget = {
        BlackholeTarget(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(input))
        )
    }
}
