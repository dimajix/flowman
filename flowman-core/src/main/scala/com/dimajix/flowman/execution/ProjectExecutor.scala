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

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.flowman.namespace.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.MappingIdentifier


private[execution] class ProjectExecutor(_parent:Executor, _project:Project, context:Context)
    extends AbstractExecutor(_parent.session, context) {
    override protected val logger = LoggerFactory.getLogger(classOf[ProjectExecutor])

    /**
      * Returns the project of this executor
      *
      * @return
      */
    override def project : Project = _project

    override def namespace: Namespace = _parent.namespace

    override def root: Executor = _parent.root

    /**
      * Creates an instance of a table of a Dataflow, or retrieves it from cache
      *
      * @param tableName
      */
    override def instantiate(tableName: MappingIdentifier) : DataFrame = {
        if (tableName.project.forall(_ == _project.name))
            cache.getOrElseUpdate((_project.name, tableName.name), createTable(tableName.name))
        else
            _parent.instantiate(tableName)
    }

    /**
      * Cleans up the session
      */
    override def cleanup() : Unit = {
    }

    /**
      * Returns a named table created by an executor. If a project is specified, Executors for other projects
      * will be searched as well
      *
      * @param identifier
      * @return
      */
    override def getTable(identifier: MappingIdentifier): DataFrame = {
        if (identifier.project.forall(_ == _project.name))
            _parent.getTable(MappingIdentifier(identifier.name, _project.name))
        else
            _parent.getTable(identifier)
    }

    /**
      * Returns the DataFrame cache of Mappings used in this Executor hierarchy.
      * @return
      */
    protected[execution] override def cache : mutable.Map[(String,String),DataFrame] = _parent.cache

    /**
      * Instantiates a table and recursively all its dependencies
      *
      * @param tableName
      * @return
      */
    private def createTable(tableName: String): DataFrame = {
        implicit val icontext = context

        // Lookup table definition
        val transform = context.getMapping(MappingIdentifier(tableName, None))
        if (transform == null) {
            logger.error(s"Mapping '${_project.name}/$tableName' not found")
            throw new NoSuchElementException(s"Mapping '${_project.name}/$tableName' not found")
        }

        // Ensure all dependencies are instantiated
        logger.info(s"Ensuring dependencies for mapping '${_project.name}/$tableName'")
        val dependencies = transform.dependencies.map(d => (d, instantiate(d))).toMap

        // Process table and register result as temp table
        val doBroadcast = transform.broadcast
        val cacheLevel = transform.cache
        logger.info(s"Instantiating table for mapping '${_project.name}/$tableName' (broadcast=$doBroadcast, cache=$cacheLevel)")
        val instance = transform.execute(this, dependencies)

        // Optionally cache the DataFrame
        if (cacheLevel != null && cacheLevel != StorageLevel.NONE)
            instance.persist(cacheLevel)

        // Optionally mark DataFrame to be broadcasted
        val df = if (doBroadcast)
            broadcast(instance)
        else
            instance

        cache.put((_project.name,tableName), df)
        df
    }
}
