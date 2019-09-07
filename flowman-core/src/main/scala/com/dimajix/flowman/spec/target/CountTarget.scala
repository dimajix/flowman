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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.ResourceIdentifier


case class CountTarget(
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
      * Build the "count" target by printing the number of records onto the console
      *
      * @param executor
      */
    override def build(executor:Executor) : Unit = {
        require(executor != null)

        val mapping = context.getMapping(this.mapping.mapping)
        val dfIn = executor.instantiate(mapping, this.mapping.output)
        val count = dfIn.count()
        System.out.println(s"Mapping '$mapping' contains $count records")
    }

    /**
      * Cleaning a count target is a no-op
      * @param executor
      */
    override def truncate(executor: Executor): Unit = { }

    override def destroy(executor: Executor) : Unit = {}
}


class CountTargetSpec extends TargetSpec {
    @JsonProperty(value = "input", required=true) private var input:String = _

    override def instantiate(context: Context): CountTarget = {
        CountTarget(
            Target.Properties(context),
            MappingOutputIdentifier.parse(context.evaluate(input))
        )
    }
}
