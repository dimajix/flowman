/*
 * Copyright 2019 Kaya Kupferschmidt
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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec.ResourceIdentifier


case class HiveDatabaseTarget(
    instanceProperties:Target.Properties,
    database: String
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[HiveDatabaseTarget])

    /**
      * Returns a list of physical resources produced by this target
      *
      * @return
      */
    override def provides(phase:Phase) : Seq[ResourceIdentifier] = {
        phase match {
            case Phase.CREATE | Phase.DESTROY => Seq(ResourceIdentifier("hiveDatabase", database))
            case _ => Seq()
        }
    }

    /**
      * Returns a list of physical resources required by this target
      *
      * @return
      */
    override def requires(phase: Phase) : Seq[ResourceIdentifier] = Seq()

    /**
      * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
      * will not provide the data itself, it will only create the container
      *
      * @param executor
      */
    override def create(executor: Executor): Unit = {
        require(executor != null)

        logger.info(s"Creating Hive database '$database'")
        executor.catalog.createDatabase(database, true)
    }

    override def migrate(executor: Executor): Unit = {}

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    override def build(executor: Executor): Unit = {}

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override def verify(executor: Executor): Unit = {
        require(executor != null)

        if (!executor.catalog.databaseExists(database)) {
            logger.error(s"Database '$database' provided by target '$identifier' does not exist")
            throw new VerificationFailedException(identifier)
        }
    }

    /**
      * Deletes data of a specific target
      *
      * @param executor
      */
    override def truncate(executor: Executor): Unit = {

    }

    /**
      * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
      * the table definition
      *
      * @param executor
      */
    override def destroy(executor: Executor): Unit = {
        require(executor != null)

        logger.info(s"Creating Hive database '$database'")
        executor.catalog.dropDatabase(database, true)
    }
}


class HiveDatabaseTargetSpec extends TargetSpec {
    @JsonProperty(value="database", required=true) private var database:String = _

    override def instantiate(context: Context): HiveDatabaseTarget = {
        HiveDatabaseTarget(
            instanceProperties(context),
            context.evaluate(database)
        )
    }
}
