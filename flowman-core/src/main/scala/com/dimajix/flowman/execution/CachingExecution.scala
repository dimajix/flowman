/*
 * Copyright (C) 2018 The Flowman Authors
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

import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.concurrent.TimeoutException
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.TaskSupport
import scala.concurrent.Await
import scala.concurrent.Awaitable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel

import com.dimajix.common.IdentityHashMap
import com.dimajix.common.SynchronizedMap
import com.dimajix.flowman.common.ThreadUtils
import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.StructType


abstract class CachingExecution(parent:Option[Execution], isolated:Boolean) extends AbstractExecution {
    private lazy val taskSupport:TaskSupport = {
        parent match {
            case Some(ce:CachingExecution) if !isolated =>
                ce.taskSupport
            case _ =>
                val tp = ThreadUtils.newForkJoinPool("execution", parallelism)
                new ForkJoinTaskSupport(tp)
        }
    }
    private lazy val parallelism = flowmanConf.getConf(FlowmanConf.EXECUTION_MAPPING_PARALLELISM)
    private lazy val timeout = flowmanConf.getConf(FlowmanConf.EXECUTION_WAIT_TIMEOUT)
    private lazy val useMappingSchemaCache = flowmanConf.getConf(FlowmanConf.EXECUTION_MAPPING_SCHEMA_CACHE)
    private lazy val useRelationSchemaCache = flowmanConf.getConf(FlowmanConf.EXECUTION_RELATION_SCHEMA_CACHE)

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

    private val mappingSchemaCache:SynchronizedMap[Mapping,Map[String,StructType]] = {
        parent match {
            case Some(ce:CachingExecution) if !isolated =>
                ce.mappingSchemaCache
            case _ =>
                SynchronizedMap(IdentityHashMap[Mapping,Map[String,StructType]]())
        }
    }
    private val mappingSchemaCacheFutures:SynchronizedMap[Mapping,Future[Map[String,StructType]]] = {
        parent match {
            case Some(ce:CachingExecution) if !isolated =>
                ce.mappingSchemaCacheFutures
            case _ =>
                SynchronizedMap(IdentityHashMap[Mapping,Future[Map[String,StructType]]]())
        }
    }

    private val relationSchemaCache:SynchronizedMap[Relation,StructType] = {
        parent match {
            case Some(ce:CachingExecution) if !isolated =>
                ce.relationSchemaCache
            case _ =>
                SynchronizedMap(IdentityHashMap[Relation,StructType]())
        }
    }
    private val relationSchemaCacheFutures:SynchronizedMap[Relation,Future[StructType]] = {
        parent match {
            case Some(ce:CachingExecution) if !isolated =>
                ce.relationSchemaCacheFutures
            case _ =>
                SynchronizedMap(IdentityHashMap[Relation,Future[StructType]]())
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
    @throws[InstantiateMappingFailedException]
    override def instantiate(mapping:Mapping) : Map[String,DataFrame] = {
        require(mapping != null)

        // We do not simply call getOrElseUpdate, since the creation of the DataFrame might be slow
        def createOrWait() : Map[String,DataFrame] = {
            val p = Promise[Map[String,DataFrame]]()
            val f = frameCacheFutures.getOrElseUpdate(mapping, p.future)
            // Check if the returned future is the one we passed in. If that is the case, the current thread
            // is responsible for fulfilling the promise
            if (f eq p.future) {
                fullfill(p, createTables(mapping))
            }
            else {
                // Other threads simply wait for the promise to be fulfilled.
                def info : String = {
                    val futuresWithoutCache = (frameCacheFutures.keys -- frameCache.keys).toSeq
                        .map(m => " - " + m.identifier.toString)
                        .sorted
                        .mkString("\n")
                    val uncompletedFutures = frameCacheFutures.toSeq
                        .filter(f => !f._2.isCompleted)
                        .map(f => " - " + f._1.identifier.toString)
                        .sorted
                        .mkString("\n")
                    s"""Instantiating mapping '${mapping.identifier}'.
                       |List of mappings with futures without cache entry:
                       |$futuresWithoutCache
                       |List of mappings with uncompleted futures:
                       |$uncompletedFutures""".stripMargin
                }
                await(f, info)
            }
        }

        frameCache.getOrElseUpdate(mapping, createOrWait())
    }

    /**
     * Returns the schema for a specific output created by a specific mapping. Note that not all mappings support
     * schema analysis beforehand. In such cases, None will be returned.
     * @param mapping
     * @return
     */
    @throws[DescribeMappingFailedException]
    override def describe(mapping:Mapping) : Map[String,StructType] = {
        // We do not simply call getOrElseUpdate, since the creation of the Schema might be slow
        def createOrWait() : Map[String,StructType] = {
            val p = Promise[Map[String,StructType]]()
            val f = mappingSchemaCacheFutures.getOrElseUpdate(mapping, p.future)
            // Check if the returned future is the one we passed in. If that is the case, the current thread
            // is responsible for fulfilling the promise
            if (f eq p.future) {
                fullfill(p, describeMapping(mapping))
            }
            else {
                // Other threads simply wait for the promise to be fulfilled.
                await(f, s"Describing mapping '${mapping.identifier}'")
            }
        }

        if (useMappingSchemaCache) {
            mappingSchemaCache.getOrElseUpdate(mapping, createOrWait())
        }
        else {
            describeMapping(mapping)
        }
    }

    @throws[DescribeMappingFailedException]
    private def describeMapping(mapping:Mapping) : Map[String,StructType] = {
        val context = mapping.context

        def dep(input: MappingOutputIdentifier) = {
            require(input.mapping.nonEmpty)

            val mapping = context.getMapping(input.mapping)
            val structs = describe(mapping, input.output)
            (input, structs)
        }

        val dependencies = {
            val inputs = mapping.inputs.toSeq
            if (inputs.size > 1 && parallelism > 1) {
                val parInputs = inputs.par
                parInputs.tasksupport = taskSupport
                parInputs.map(dep).seq.toMap
            }
            else {
                inputs.map(dep).toMap
            }
        }

        // Transform any non-fatal exception in a DescribeMappingFailedException
        try {
            logger.info(s"Describing mapping '${mapping.identifier}'")
            listeners.foreach { l =>
                Try {
                    l._1.describeMapping(this, mapping, l._2)
                }
            }
            mapping.describe(this, dependencies)
        }
        catch {
            case NonFatal(e) => throw new DescribeMappingFailedException(mapping.identifier, e)
        }
    }

    /**
     * Returns the schema for a specific relation
     * @param relation
     * @param partitions
     * @return
     */
    @throws[DescribeRelationFailedException]
    override def describe(relation:Relation, partitions:Map[String,FieldValue] = Map()) : StructType = {
        // We do not simply call getOrElseUpdate, since the creation of the Schema might be slow
        def createOrWait() : StructType = {
            val p = Promise[StructType]()
            val f = relationSchemaCacheFutures.getOrElseUpdate(relation, p.future)
            // Check if the returned future is the one we passed in. If that is the case, the current thread
            // is responsible for fulfilling the promise
            if (f eq p.future) {
                fullfill(p, describeRelation(relation, partitions))
            }
            else {
                // Other threads simply wait for the promise to be fulfilled.
                await(f, s"Describing relation '${relation.identifier}'")
            }
        }

        if (useRelationSchemaCache) {
            try {
                relationSchemaCache.getOrElseUpdate(relation, createOrWait())
            }
            finally {
                // Remove relation from Futures, so we have another chance when the relation is described again
                // with a possibly different partition information
                relationSchemaCacheFutures.remove(relation)
            }
        }
        else {
            describeRelation(relation, partitions)
        }
    }

    @throws[DescribeRelationFailedException]
    private def describeRelation(relation:Relation, partitions:Map[String,FieldValue] = Map()) : StructType = {
        try {
            logger.info(s"Describing relation '${relation.identifier}'")
            listeners.foreach { l =>
                Try {
                    l._1.describeRelation(this, relation, l._2)
                }
            }
            relation.describe(this, partitions)
        }
        catch {
            case NonFatal(e) => throw new DescribeRelationFailedException(relation.identifier, e)
        }
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

        // Invalidate schema caches
        relationSchemaCache.toSeq
            .map(_._1)
            .filter(_.provides(Operation.CREATE).exists(_.contains(key)))
            .foreach(relationSchemaCache.impl.remove)
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
            frameCacheFutures.clear()
            mappingSchemaCache.clear()
            mappingSchemaCacheFutures.clear()
            relationSchemaCache.clear()
            relationSchemaCacheFutures.clear()
            resources.clear()
        }
    }

    /**
     * Instantiates a table and recursively all its dependencies
     *
     * @param mapping
     * @return
     */
    @throws[NoSuchMappingOutputException]
    @throws[InstantiateMappingFailedException]
    private def createTables(mapping:Mapping): Map[String,DataFrame] = {
        val context = mapping.context

        def dep(inputs:MappingOutputIdentifier) = {
            require(inputs.mapping.nonEmpty)

            val mapping = context.getMapping(inputs.mapping)
            if (!mapping.outputs.contains(inputs.output))
                throw new NoSuchMappingOutputException(mapping.identifier, inputs.output)
            val instances = instantiate(mapping)
            (inputs, instances(inputs.output))
        }

        val dependencies = {
            val inputs = mapping.inputs.toSeq
            if (inputs.size > 1 && parallelism > 1) {
                val parInputs = inputs.par
                parInputs.tasksupport = taskSupport
                parInputs.map(dep).seq.toMap
            }
            else {
                inputs.map(dep).toMap
            }
        }

        // Retry cache (maybe it was inserted via dependencies)
        frameCache.getOrElseUpdate(mapping, createTables(mapping, dependencies))
    }

    @throws[InstantiateMappingFailedException]
    private def createTables(mapping: Mapping, dependencies:Map[MappingOutputIdentifier, DataFrame]) : Map[String,DataFrame] = {
        // Process table and register result as temp table
        val doBroadcast = mapping.broadcast
        val doCheckpoint = mapping.checkpoint
        val cacheLevel = mapping.cache
        val cacheDesc = if (cacheLevel == null || cacheLevel == StorageLevel.NONE) "None" else cacheLevel.description
        logger.info(s"Instantiating mapping '${mapping.identifier}' with outputs ${mapping.outputs.map("'" + _ + "'").mkString(",")} (broadcast=$doBroadcast, cache='$cacheDesc')")
        listeners.foreach { l =>
            Try {
                l._1.instantiateMapping(this, mapping, l._2)
            }
        }

        // Transform any non-fatal exception in a InstantiateMappingFailedException
        try {
            val instances = mapping.execute(this, dependencies)

            // Optionally checkpoint DataFrame
            val df1 = if (doCheckpoint)
                instances.map { case (name, df) => (name, df.checkpoint(false)) }
            else
                instances

            // Optionally cache the DataFrames, before potentially marking them as broadcast candidates
            if (cacheLevel != null && cacheLevel != StorageLevel.NONE) {
                // If one of the DataFrame is called 'cache', then only cache that one, otherwise all will be cached
                if (df1.keySet.contains("cache"))
                    df1("cache").persist(cacheLevel)
                else
                    df1.values.foreach(_.persist(cacheLevel))
            }

            // Optionally mark DataFrame to be broadcasted
            val df2 = if (doBroadcast)
                df1.map { case (name, df) => (name, broadcast(df)) }
            else
                df1

            df2
        }
        catch {
            case NonFatal(e) => throw new InstantiateMappingFailedException(mapping.identifier, e)
        }
    }

    private def fullfill[T](promise:Promise[T], fn: => T) : T = {
        // Do not use Scalas Try, since this will only catch NonFatal errors. This will lead to a dead-lock
        // in case of Fatal errors
        val result = try {
            fn
        }
        catch {
            case ex: Throwable =>
                promise.failure(ex)
                throw ex
        }
        promise.success(result)
        result
    }

    private def await[T](awaitable: Awaitable[T], description: => String) : T = {
        await(awaitable, Duration.Zero, description)
    }

    @tailrec
    private def await[T](awaitable: Awaitable[T], totalDuration:Duration, description: => String): T = {
        val timeout = Duration(this.timeout, TimeUnit.SECONDS)
        try {
            Await.result(awaitable, timeout)
        }
        catch {
            case _: TimeoutException =>
                val newTotalDuration = totalDuration + timeout
                logger.warn(s"Already waiting ${newTotalDuration.toSeconds} seconds for: $description")
                await(awaitable, newTotalDuration, description)
        }
    }
}
