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

import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.TargetIdentifier


abstract class BaseTarget extends Target {
    protected override def instanceProperties : Target.Properties

    /**
      * Returns an identifier for this target
      * @return
      */
    override def identifier : TargetIdentifier = TargetIdentifier(name, Option(project).map(_.name))

    /**
      * Returns true if the output should be executed per default
      * @return
      */
    override def enabled : Boolean = instanceProperties.enabled

    /**
      * Returns an instance representing this target with the context
      * @return
      */
    override def instance : TargetInstance = {
        TargetInstance(
            Option(namespace).map(_.name).getOrElse(""),
            Option(project).map(_.name).getOrElse(""),
            name
        )
    }

    /**
      * Returns an explicit user defined list of targets to be executed after this target. I.e. this
      * target needs to be executed before all other targets in this list.
      * @return
      */
    override def before : Seq[TargetIdentifier] = instanceProperties.before

    /**
      * Returns an explicit user defined list of targets to be executed before this target I.e. this
      * * target needs to be executed after all other targets in this list.
      *
      * @return
      */
    override def after : Seq[TargetIdentifier] = instanceProperties.after


    /**
      * Returns a list of physical resources produced by this target
      *
      * @return
      */
    override def provides(phase: Phase): Seq[ResourceIdentifier] = Seq()

    /**
      * Returns a list of physical resources required by this target
      *
      * @return
      */
    override def requires(phase: Phase): Seq[ResourceIdentifier] = Seq()
/**
      * Executes a specific phase of this target
      * @param executor
      * @param phase
      */
    override def execute(executor: Executor, phase: Phase) : Unit = {
        phase match {
            case Phase.CREATE => create(executor)
            case Phase.MIGRATE => migrate(executor)
            case Phase.BUILD => build(executor)
            case Phase.VERIFY => verify(executor)
            case Phase.TRUNCATE => truncate(executor)
            case Phase.DESTROY => destroy(executor)
        }
    }

    /**
      * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
      * will not provide the data itself, it will only create the container
      * @param executor
      */
    protected def create(executor:Executor) : Unit = {}
    protected def migrate(executor:Executor) : Unit = {}

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    protected def build(executor:Executor) : Unit = {}

    /**
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    protected def verify(executor: Executor) : Unit = {}

    /**
      * Deletes data of a specific target
      *
      * @param executor
      */
    protected def truncate(executor:Executor) : Unit = {}

    /**
      * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
      * the table definition
      * @param executor
      */
    protected def destroy(executor:Executor) : Unit = {}
}
