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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import com.dimajix.common.IdentityHashMap
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.flow.Mapping


class RootExecutor private(session:Session, sharedCache:Executor, isolated:Boolean)
    extends AbstractExecutor(session) {
    override protected val logger = LoggerFactory.getLogger(classOf[RootExecutor])

    private val _cache = {
        if (sharedCache != null) {
            if (isolated)
                IdentityHashMap[Mapping,Map[String,DataFrame]]()
            else
                sharedCache.cache
        }
        else {
            IdentityHashMap[Mapping,Map[String,DataFrame]]()
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
    override def instantiate(mapping:Mapping) : Map[String,DataFrame] = {
        require(mapping != null)

        cache.getOrElseUpdate(mapping, createTables(mapping))
    }

    /**
      * Perform Spark related cleanup operations (like deregistering temp tables, clearing caches, ...)
      */
    override def cleanup(): Unit = {
        logger.info("Cleaning up cached Spark tables")
        if (sparkRunning) {
            val catalog = spark.catalog
            catalog.clearCache()
        }

        logger.info("Cleaning up cached Spark data frames in executor")
        cache.values.foreach(_.values.foreach(_.unpersist(true)))
        cache.clear()
    }

    /**
      * Returns the DataFrame cache of Mappings used in this Executor hierarchy.
      * @return
      */
    protected[execution] override def cache : IdentityHashMap[Mapping,Map[String,DataFrame]] = _cache

    /**
      * Instantiates a table and recursively all its dependencies
      *
      * @param mapping
      * @return
      */
    private def createTables(mapping:Mapping): Map[String,DataFrame] = {
        // Ensure all dependencies are instantiated
        logger.info(s"Ensuring dependencies for mapping '${mapping.identifier}'")

        val context = mapping.context
        val dependencies = mapping.dependencies.map { dep =>
            require(dep.mapping.nonEmpty)

            val mapping = context.getMapping(dep.mapping)
            if (!mapping.outputs.contains(dep.output))
                throw new NoSuchElementException(s"Mapping ${mapping.identifier} does mot produce output '${dep.output}'")
            val instances = instantiate(mapping)
            (dep, instances(dep.output))
        }.toMap

        // Retry cache (maybe it was inserted via dependencies)
        cache.getOrElseUpdate(mapping, createTables(mapping, dependencies))
    }

    private def createTables(mapping: Mapping, dependencies:Map[MappingOutputIdentifier, DataFrame]) = {
        // Process table and register result as temp table
        val doBroadcast = mapping.broadcast
        val doCheckpoint = mapping.checkpoint
        val cacheLevel = mapping.cache
        val cacheDesc = if (cacheLevel == null || cacheLevel == StorageLevel.NONE) "None" else cacheLevel.description
        logger.info(s"Instantiating mapping '${mapping.identifier}' with outputs ${mapping.outputs.map("'" + _ + "'").mkString(",")} (broadcast=$doBroadcast, cache='$cacheDesc')")
        val instances = mapping.execute(this, dependencies)

        // Optionally checkpoint DataFrame
        val df1 = if (doCheckpoint)
            instances.map { case (name,df) => (name, df.checkpoint(false)) }
        else
            instances

        // Optionally mark DataFrame to be broadcasted
        val df2 = if (doBroadcast)
            df1.map { case (name,df) => (name, broadcast(df)) }
        else
            df1

        // Optionally cache the DataFrame
        if (cacheLevel != null && cacheLevel != StorageLevel.NONE)
            df2.values.foreach(_.persist(cacheLevel))

        df2
    }
}
