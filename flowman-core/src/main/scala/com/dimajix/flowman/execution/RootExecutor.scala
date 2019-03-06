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

import java.util.NoSuchElementException

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Namespace


class RootExecutor private(session:Session, context:Context, sharedCache:Executor, isolated:Boolean)
    extends AbstractExecutor(session, context) {
    override protected val logger = LoggerFactory.getLogger(classOf[RootExecutor])

    private val _cache = {
        if (sharedCache != null) {
            if (isolated)
                mutable.Map[(String,String),DataFrame]()
            else
                sharedCache.cache
        }
        else {
            mutable.Map[(String,String),DataFrame]()
        }
    }
    private val _children = mutable.Map[String,ProjectExecutor]()
    private val _namespace = context.namespace

    def this(session: Session, context:Context) = {
        this(session, context, null, true)
    }
    def this(parent:Executor, context:Context, isolated:Boolean) = {
        this(parent.session, context, parent, isolated)
    }

    /**
      * Returns the Namespace of the Executor
      *
      * @return
      */
    override def namespace : Namespace = _namespace

    override def project: Project = null

    override def root: Executor = this

     /**
      * Returns a fully qualified table as a DataFrame from a project belonging to the namespace of this executor
      *
      * @param identifier
      * @return
      */
    override def getTable(identifier: MappingIdentifier): DataFrame = {
        require(identifier != null)

        if (identifier.project.isEmpty)
            throw new NoSuchElementException("Expected project name in table specifier")
        cache.getOrElse((identifier.project.get, identifier.name), throw new NoSuchElementException(s"Table $identifier not found"))
    }

    /**
      * Creates an instance of a table of a Dataflow, or retrieves it from cache
      *
      * @param identifier
      */
    override def instantiate(identifier: MappingIdentifier): DataFrame = {
        require(identifier != null)

        if (identifier.project.isEmpty)
            throw new NoSuchElementException("Expected project name in table specifier")
        val child = getProjectExecutor(identifier.project.get)
        child.instantiate(MappingIdentifier(identifier.name, None))
    }

    /**
      * Perform Spark related cleanup operations (like deregistering temp tables, clearing caches, ...)
      */
    override def cleanup(): Unit = {
        logger.info("Cleaning up root executor and all children")
        if (sparkRunning) {
            val catalog = spark.catalog
            catalog.clearCache()
        }
        _children.values.foreach(_.cleanup())
    }

    /**
      * Returns the DataFrame cache of Mappings used in this Executor hierarchy.
      * @return
      */
    protected[execution] override def cache : mutable.Map[(String,String),DataFrame] = _cache

    def getProjectExecutor(project:Project) : ProjectExecutor = {
        require(project != null)
        _children.getOrElseUpdate(project.name, createProjectExecutor(project))
    }
    def getProjectExecutor(name:String) : ProjectExecutor = {
        require(name != null && name.nonEmpty)
        _children.getOrElseUpdate(name, createProjectExecutor(name))
    }

    private def createProjectExecutor(project:Project) : ProjectExecutor = {
        val pcontext = context.getProjectContext(project)
        val executor = new ProjectExecutor(this, project, pcontext)
        _children.update(project.name, executor)
        executor
    }
    private def createProjectExecutor(projectName:String) : ProjectExecutor = {
        val pcontext = context.getProjectContext(projectName)
        val project = pcontext.project
        val executor = new ProjectExecutor(this, project, pcontext)
        _children.update(project.name, executor)
        executor
    }
}
