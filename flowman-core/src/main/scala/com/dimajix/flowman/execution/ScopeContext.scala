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

package com.dimajix.flowman.execution

import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.TargetIdentifier
import com.dimajix.flowman.spec.connection.Connection
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spec.task.Job


object ScopeContext {
    class Builder(parent:Context) extends AbstractContext.Builder(parent, SettingLevel.SCOPE_OVERRIDE) {
        override protected val logger = LoggerFactory.getLogger(classOf[ScopeContext])

        override protected def createContext(env:Map[String,(Any, Int)], config:Map[String,(String, Int)], connections:Map[String, ConnectionSpec]) : ScopeContext = {
            new ScopeContext(parent, env, config)
        }
    }
}


class ScopeContext(
    parent:Context,
    fullEnv:Map[String,(Any, Int)],
    fullConfig:Map[String,(String, Int)]
) extends AbstractContext(parent, fullEnv, fullConfig) {
    override def namespace: Namespace = parent.namespace
    override def project: Project = parent.project
    override def root: Context = parent.root
    override def getConnection(identifier: ConnectionIdentifier): Connection = parent.getConnection(identifier)
    override def getMapping(identifier: MappingIdentifier): Mapping = parent.getMapping(identifier)
    override def getRelation(identifier: RelationIdentifier): Relation = parent.getRelation(identifier)
    override def getTarget(identifier: TargetIdentifier): Target = parent.getTarget(identifier)
    override def getJob(identifier: JobIdentifier): Job = parent.getJob(identifier)
    override def getProjectContext(projectName: String): Context = parent.getProjectContext(projectName)
    override def getProjectContext(project: Project): Context = parent.getProjectContext(project)
}
