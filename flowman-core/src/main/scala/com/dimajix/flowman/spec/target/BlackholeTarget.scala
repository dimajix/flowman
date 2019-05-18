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

import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier


case class BlackholeTarget(
    instanceProperties:Target.Properties
) extends BaseTarget {
    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override def build(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : Unit = {
        input(instanceProperties.input).write.format("null").save()
    }

    /**
      * "Cleaning" a blackhole essentially is a no-op
      * @param executor
      */
    override def clean(executor: Executor): Unit = {

    }
}



class BlackholeTargetSpec extends TargetSpec {
    override def instantiate(context: Context): BlackholeTarget = {
        BlackholeTarget(
            Target.Properties(context)
        )
    }
}