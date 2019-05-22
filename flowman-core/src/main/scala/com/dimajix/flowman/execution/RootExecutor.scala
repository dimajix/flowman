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

package com.dimajix.flowman.execution

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.flow.Mapping


class RootExecutor private(session:Session, sharedCache:Executor, isolated:Boolean)
    extends AbstractExecutor(session) {
    override protected val logger = LoggerFactory.getLogger(classOf[RootExecutor])

    private val _cache = {
        if (sharedCache != null) {
            if (isolated)
                mutable.Map[MappingIdentifier,DataFrame]()
            else
                sharedCache.cache
        }
        else {
            mutable.Map[MappingIdentifier,DataFrame]()
        }
    }
    private val _namespace = session.namespace

    def this(session: Session) = {
        this(session, null, true)
    }
    def this(parent:Executor, isolated:Boolean) = {
        this(parent.session, parent, isolated)
    }

    /**
      * Returns the Namespace of the Executor
      *
      * @return
      */
    override def namespace : Namespace = _namespace

    override def root: Executor = this

    /**
      * Creates an instance of a mapping, or retrieves it from cache
      *
      * @param mapping
      */
    override def instantiate(mapping:Mapping) : DataFrame = {
        require(mapping != null)

        cache.getOrElseUpdate(mapping.identifier, createTable(mapping))
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
    }

    /**
      * Returns the DataFrame cache of Mappings used in this Executor hierarchy.
      * @return
      */
    protected[execution] override def cache : mutable.Map[MappingIdentifier,DataFrame] = _cache

    /**
      * Instantiates a table and recursively all its dependencies
      *
      * @param mapping
      * @return
      */
    private def createTable(mapping:Mapping): DataFrame = {
        // Ensure all dependencies are instantiated
        logger.info(s"Ensuring dependencies for mapping '${mapping.identifier}'")
        val context = mapping.context
        val dependencies = mapping.dependencies.map(d => (d, instantiate(context.getMapping(d)))).toMap

        // Process table and register result as temp table
        val doBroadcast = mapping.broadcast
        val doCheckpoint = mapping.checkpoint
        val cacheLevel = mapping.cache
        val cacheDesc = if (cacheLevel == null || cacheLevel == StorageLevel.NONE) "None" else cacheLevel.description
        logger.info(s"Instantiating table for mapping '${mapping.identifier}' (broadcast=$doBroadcast, cache='$cacheDesc')")
        val instance = mapping.execute(this, dependencies)

        // Optionally checkpoint DataFrame
        val df1 = if (doCheckpoint)
            instance.checkpoint(false)
        else
            instance

        // Optionally mark DataFrame to be broadcasted
        val df2 = if (doBroadcast)
            broadcast(df1)
        else
            df1

        // Optionally cache the DataFrame
        if (cacheLevel != null && cacheLevel != StorageLevel.NONE)
            df2.persist(cacheLevel)

        cache.put(mapping.identifier, df2)
        df2
    }
}
