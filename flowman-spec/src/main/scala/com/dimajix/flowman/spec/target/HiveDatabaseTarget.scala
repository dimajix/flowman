/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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
import org.slf4j.LoggerFactory

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target


case class HiveDatabaseTarget(
    instanceProperties:Target.Properties,
    database: String
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[HiveDatabaseTarget])

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.CREATE, Phase.VERIFY, Phase.DESTROY)

    /**
      * Returns a list of physical resources produced by this target
      *
      * @return
      */
    override def provides(phase:Phase) : Set[ResourceIdentifier] = {
        phase match {
            case Phase.CREATE | Phase.DESTROY => Set(ResourceIdentifier.ofHiveDatabase(database))
            case _ => Set()
        }
    }

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase) : Trilean = {
        phase match {
            case Phase.CREATE => !execution.catalog.databaseExists(database)
            case Phase.VERIFY => Yes
            case Phase.DESTROY => execution.catalog.databaseExists(database)
            case _ => No
        }
    }

    /**
      * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
      * will not provide the data itself, it will only create the container
      *
      * @param executor
      */
    override def create(executor: Execution): Unit = {
        require(executor != null)

        logger.info(s"Creating Hive database '$database'")
        executor.catalog.createDatabase(database, true)
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override def verify(executor: Execution): Unit = {
        require(executor != null)

        if (!executor.catalog.databaseExists(database)) {
            logger.error(s"Database '$database' provided by target '$identifier' does not exist")
            throw new VerificationFailedException(identifier)
        }
    }

    /**
      * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
      * the table definition
      *
      * @param executor
      */
    override def destroy(executor: Execution): Unit = {
        require(executor != null)

        logger.info(s"Creating Hive database '$database'")
        executor.catalog.dropDatabase(database, true)
    }
}


class HiveDatabaseTargetSpec extends TargetSpec {
    @JsonProperty(value="database", required=true) private var database:String = _

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): HiveDatabaseTarget = {
        HiveDatabaseTarget(
            instanceProperties(context, properties),
            context.evaluate(database)
        )
    }
}
