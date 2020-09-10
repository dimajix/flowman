/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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
import org.slf4j.Logger

import com.dimajix.common.IdentityHashMap
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.StructType


abstract class CachingExecutor(parent:Option[Executor], isolated:Boolean) extends Executor {
    protected val logger:Logger

    private val frameCache:IdentityHashMap[Mapping,Map[String,DataFrame]] = {
        parent match {
            case Some(ce:CachingExecutor) if !isolated =>
                ce.frameCache
            case _ =>
                IdentityHashMap[Mapping,Map[String,DataFrame]]()
        }
    }

    private val schemaCache:mutable.Map[MappingOutputIdentifier, StructType] = {
        parent match {
            case Some(ce:CachingExecutor) if !isolated =>
                ce.schemaCache
            case _ =>
                mutable.Map[MappingOutputIdentifier, StructType]()
        }
    }

    /**
     * Creates an instance of a mapping, or retrieves it from cache
     *
     * @param mapping
     */
    override def instantiate(mapping:Mapping) : Map[String,DataFrame] = {
        require(mapping != null)

        frameCache.getOrElseUpdate(mapping, createTables(mapping))
    }

    /**
     * Returns the schema for a specific output created by a specific mapping. Note that not all mappings support
     * schema analysis beforehand. In such cases, None will be returned.
     * @param mapping
     * @param output
     * @return
     */
    override def describe(mapping:Mapping, output:String) : StructType = {
        val oid = MappingOutputIdentifier(mapping.identifier, output)
        schemaCache.getOrElseUpdate(oid, {
            if (!mapping.outputs.contains(output))
                throw new NoSuchMappingOutputException(oid)
            val context = mapping.context
            val deps = mapping.inputs
                .map(id => id -> describe(context.getMapping(id.mapping), id.output))
                .toMap

            mapping.describe(this, deps, output)
        })
    }

    /**
     * Releases any temporary tables
     */
    override def cleanup() : Unit = {
        frameCache.values.foreach(_.values.foreach(_.unpersist(true)))
        frameCache.clear()
        schemaCache.clear()
    }

    /**
     * Instantiates a table and recursively all its dependencies
     *
     * @param mapping
     * @return
     */
    private def createTables(mapping:Mapping): Map[String,DataFrame] = {
        // Ensure all dependencies are instantiated
        logger.debug(s"Ensuring dependencies for mapping '${mapping.identifier}'")

        val context = mapping.context
        val dependencies = mapping.inputs.map { dep =>
            require(dep.mapping.nonEmpty)

            val mapping = context.getMapping(dep.mapping)
            if (!mapping.outputs.contains(dep.output))
                throw new NoSuchMappingOutputException(mapping.identifier, dep.output)
            val instances = instantiate(mapping)
            (dep, instances(dep.output))
        }.toMap

        // Retry cache (maybe it was inserted via dependencies)
        frameCache.getOrElseUpdate(mapping, createTables(mapping, dependencies))
    }

    private def createTables(mapping: Mapping, dependencies:Map[MappingOutputIdentifier, DataFrame]) : Map[String,DataFrame] = {
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

        df2.foreach { case (name,df) =>
            logger.debug(s"Instantiated mapping '${mapping.identifier}' output '$name' with schema\n ${df.schema.treeString}")
        }

        df2
    }
}
