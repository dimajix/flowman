/*
 * Copyright (C) 2019 The Flowman Authors
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
import org.apache.hadoop.fs.Path

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target


final case class HiveDatabaseTarget(
    instanceProperties:Target.Properties,
    database: String,
    catalog: String = "",
    location: Option[Path] = None
) extends BaseTarget {
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
      * @param execution
      */
    override def create(execution: Execution): Unit = {
        require(execution != null)

        logger.info(s"Creating Hive database '$database'")
        execution.catalog.createDatabase(database, catalog, location, true)
    }

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param execution
      */
    override def verify(execution: Execution): Unit = {
        require(execution != null)

        if (!execution.catalog.databaseExists(database)) {
            val error = s"Database '$database' provided by target '$identifier' does not exist"
            logger.error(error)
            throw new VerificationFailedException(identifier, new ExecutionException(error))
        }
    }

    /**
      * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
      * the table definition
      *
      * @param execution
      */
    override def destroy(execution: Execution): Unit = {
        require(execution != null)

        logger.info(s"Dropping Hive database '$database'")
        execution.catalog.dropDatabase(database, true)
    }
}


class HiveDatabaseTargetSpec extends TargetSpec {
    @JsonProperty(value="database", required=true) private var database:String = _
    @JsonProperty(value="catalog", required=false) private var catalog:String = ""
    @JsonProperty(value="location", required=false) private var location:Option[String] = None

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): HiveDatabaseTarget = {
        HiveDatabaseTarget(
            instanceProperties(context, properties),
            context.evaluate(database),
            context.evaluate(catalog),
            context.evaluate(location).map(l => new Path(l))
        )
    }
}
