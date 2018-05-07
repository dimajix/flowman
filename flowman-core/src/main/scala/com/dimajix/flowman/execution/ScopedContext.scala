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

package com.dimajix.flowman.execution

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.Connection
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.OutputIdentifier
import com.dimajix.flowman.spec.Profile
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.TableIdentifier
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spec.runner.Runner
import com.dimajix.flowman.spec.task.Job


class ScopedContext(parent:Context) extends AbstractContext {
    override protected val logger: Logger = LoggerFactory.getLogger(classOf[ScopedContext])

    updateFrom(parent)

    override def namespace : Namespace = parent.namespace

    override def project : Project = parent.project

    override def root : Context = parent.root

    /**
      * Try to retrieve the specified database connection. Performs lookups in parent context if required
      *
      * @param identifier
      * @return
      */
    override def getConnection(identifier: ConnectionIdentifier): Connection = parent.getConnection(identifier)

    /**
      * Returns a specific named Mapping. The Mapping can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getMapping(identifier: TableIdentifier): Mapping = parent.getMapping(identifier)

    /**
      * Returns a specific named Relation. The Relation can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getRelation(identifier: RelationIdentifier): Relation = parent.getRelation(identifier)

    /**
      * Returns a specific named Output. The Output can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getOutput(identifier: OutputIdentifier): Output = parent.getOutput(identifier)

    /**
      * Returns a specific named Job. The Job can either be inside this Contexts project or in a different
      * project within the same namespace
      *
      * @param identifier
      * @return
      */
    override def getJob(identifier: JobIdentifier): Job = parent.getJob(identifier)

    /**
      * Returns the appropriate runner
      *
      * @return
      */
    override def runner: Runner = parent.runner

    /**
      * Creates a new chained context with additional environment variables
      * @param env
      * @return
      */
    override def withEnvironment(env:Map[String,String]) : Context = {
        withEnvironment(env.toSeq)
    }
    override def withEnvironment(env:Seq[(String,String)]) : Context = {
        setEnvironment(env, SettingLevel.SCOPE_OVERRIDE)
        this
    }

    /**
      * Creates a new chained context with additional Spark configuration variables
      * @param env
      * @return
      */
    override def withConfig(env:Map[String,String]) : Context = {
        withConfig(env.toSeq)
    }
    override def withConfig(env:Seq[(String,String)]) : Context = {
        setConfig(env, SettingLevel.SCOPE_OVERRIDE)
        this
    }

    /**
      * Creates a new chained context with additional properties from a profile
      * @param profile
      * @return
      */
    override def withProfile(profile:Profile) : Context = ???
}
