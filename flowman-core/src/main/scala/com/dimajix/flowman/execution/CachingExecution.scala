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

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel
import org.slf4j.Logger

import com.dimajix.common.IdentityHashMap
import com.dimajix.common.SynchronizedMap
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.StructType


abstract class CachingExecution(parent:Option[Execution], isolated:Boolean) extends Execution {
    protected val logger:Logger

    private val frameCache:SynchronizedMap[Mapping,Map[String,DataFrame]] = {
        parent match {
            case Some(ce:CachingExecution) if !isolated =>
                ce.frameCache
            case _ =>
                SynchronizedMap(IdentityHashMap[Mapping,Map[String,DataFrame]]())
        }
    }

    private val frameCacheFutures:SynchronizedMap[Mapping,Future[Map[String,DataFrame]]] = {
        parent match {
            case Some(ce:CachingExecution) if !isolated =>
                ce.frameCacheFutures
            case _ =>
                SynchronizedMap(IdentityHashMap[Mapping,Future[Map[String,DataFrame]]]())
        }
    }

    private val schemaCache:SynchronizedMap[Mapping,TrieMap[String,StructType]] = {
        parent match {
            case Some(ce:CachingExecution) if !isolated =>
                ce.schemaCache
            case _ =>
                SynchronizedMap(IdentityHashMap[Mapping,TrieMap[String,StructType]]())
        }
    }

    private val resources:mutable.ListBuffer[(ResourceIdentifier,() => Unit)] = {
        parent match {
            case Some(ce: CachingExecution) if !isolated =>
                ce.resources
            case _ =>
                mutable.ListBuffer[(ResourceIdentifier,() => Unit)]()
        }
    }

    /**
     * Creates an instance of a mapping, or retrieves it from cache
     *
     * @param mapping
     */
    override def instantiate(mapping:Mapping) : Map[String,DataFrame] = {
        require(mapping != null)

        // We do not simply call getOrElseUpdate, since the creation of the DataFrame might be slow and
        // concurrent trials
        def createOrWait() : Map[String,DataFrame] = {
            val p = Promise[Map[String,DataFrame]]()
            val f = frameCacheFutures.getOrElseUpdate(mapping, p.future)
            // Check if the returned future is the one we passed in. If that is the case, the current thread
            // is responsible for fullfilling the promise
            if (f eq p.future) {
                val tables = Try(createTables(mapping))
                p.complete(tables)
                tables.get
            }
            else {
                // Other threads simply wait for the promise to be fullfilled.
                Await.result(f, Duration.Inf)
            }
        }

        frameCache.getOrElseUpdate(mapping, createOrWait())
    }

    /**
     * Returns the schema for a specific output created by a specific mapping. Note that not all mappings support
     * schema analysis beforehand. In such cases, None will be returned.
     * @param mapping
     * @param output
     * @return
     */
    override def describe(mapping:Mapping, output:String) : StructType = {
        schemaCache.getOrElseUpdate(mapping, TrieMap())
            .getOrElseUpdate(output, {
                if (!mapping.outputs.contains(output))
                    throw new NoSuchMappingOutputException(mapping.identifier, output)
                val context = mapping.context
                val deps = mapping.inputs
                    .map(id => id -> describe(context.getMapping(id.mapping), id.output))
                    .toMap

                // Transform any non-fatal exception in a DescribeMappingFailedException
                try {
                    logger.info(s"Describing mapping '${mapping.identifier}' for output ${output}")
                    mapping.describe(this, deps, output)
                }
                catch {
                    case NonFatal(e) => throw new DescribeMappingFailedException(mapping.identifier, e)
                }
            })
    }

    /**
     * Registers a refresh function associated with a [[ResourceIdentifier]]
     * @param key
     * @param refresh
     */
    override def addResource(key:ResourceIdentifier)(refresh: => Unit) : Unit = {
        resources.synchronized {
            resources.append((key,() => refresh))
        }
    }

    /**
     * Invokes all refresh functions associated with a [[ResourceIdentifier]]
     * @param key
     */
    override def refreshResource(key:ResourceIdentifier) : Unit = {
        resources.synchronized {
            resources.filter(kv => kv._1.contains(key) || key.contains(kv._1)).foreach(_._2())
        }
        parent.foreach(_.refreshResource(key))
    }

    /**
     * Releases all DataFrames and all caches of DataFrames which have been created within this scope. This method
     * will not cleanup the parent Execution (if any).
     */
    override def cleanup() : Unit = {
        // Find out if we are using a shared cache. If that is the case, do not perform a cleanup operation!
        val sharedCache = parent match {
            case Some(_:CachingExecution) if !isolated => true
            case _ => false
        }
        if (!sharedCache) {
            frameCache.values.foreach(_.values.foreach(_.unpersist(true)))
            frameCache.clear()
            schemaCache.clear()
            resources.clear()
        }
    }

    /**
     * Instantiates a table and recursively all its dependencies
     *
     * @param mapping
     * @return
     */
    private def createTables(mapping:Mapping): Map[String,DataFrame] = {
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

        // Transform any non-fatal exception in a InstantiateMappingFailedException
        val instances = {
            try {
                mapping.execute(this, dependencies)
            }
            catch {
                case NonFatal(e) => throw new InstantiateMappingFailedException(mapping.identifier, e)
            }
        }

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

        // Optionally cache the DataFrames
        if (cacheLevel != null && cacheLevel != StorageLevel.NONE) {
            // If one of the DataFrame is called 'cache', then only cache that one, otherwise all will be cached
            if (df2.keySet.contains("cache"))
                df2("cache").persist(cacheLevel)
            else
                df2.values.foreach(_.persist(cacheLevel))
        }

        df2
    }
}
