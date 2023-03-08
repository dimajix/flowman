/*
 * Copyright (C) 2022 The Flowman Authors
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

import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

import org.apache.spark.sql.util.QueryExecutionListener
import org.slf4j.LoggerFactory


private sealed trait CleanupTask {
    def execute(session:Session) : Unit
}
private case class CleanQueryExecutionListener(listener: QueryExecutionListener) extends CleanupTask {
    def execute(session:Session) : Unit = {
        session.spark.listenerManager.unregister(listener)
    }
}


/**
 * A WeakReference associated with a CleanupTask.
 *
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
 */
private class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
    extends WeakReference(referent, referenceQueue)


private object SessionCleaner {
    private val REF_QUEUE_POLL_TIMEOUT = 100
}


class SessionCleaner(session:Session) {
    private val logger = LoggerFactory.getLogger(classOf[SessionCleaner])

    /**
     * A buffer to ensure that `CleanupTaskWeakReference`s are not garbage collected as long as they
     * have not been handled by the reference queue.
     */
    private val referenceBuffer =
        Collections.newSetFromMap[CleanupTaskWeakReference](new ConcurrentHashMap)

    private val referenceQueue = new ReferenceQueue[AnyRef]
    private var stopped: Boolean = false
    private val cleaningThread = new Thread() {
        override def run(): Unit = keepCleaning()
    }

    /**
     * Register a [[QueryExecutionListener]] to be automatically removed from the SparkSession once the [[listenerOwner]]
     * is to be garbage collected.
     * @param listenerOwner
     * @param listener
     */
    def registerQueryExecutionListener(listenerOwner: AnyRef, listener: QueryExecutionListener): Unit = {
        registerForCleanup(listenerOwner, CleanQueryExecutionListener(listener))
    }

    def start() : Unit = {
        logger.debug("Starting session cleaner")
        cleaningThread.setDaemon(true)
        cleaningThread.setName("Flowman Session Cleaner")
        cleaningThread.start()
    }
    def stop() : Unit = {
        logger.debug("Stopping session cleaner")
        stopped = true

        // Interrupt the cleaning thread, but wait until the current task has finished before
        // doing so. This guards against the race condition where a cleaning thread may
        // potentially clean similarly named variables created by a different SparkContext,
        // resulting in otherwise inexplicable block-not-found exceptions (SPARK-6132).
        synchronized {
            cleaningThread.interrupt()
        }
        cleaningThread.join()
    }

    private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
        referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
    }

    private def keepCleaning() : Unit = {
        while (!stopped) {
            try {
                val reference = Option(referenceQueue.remove(SessionCleaner.REF_QUEUE_POLL_TIMEOUT))
                    .map(_.asInstanceOf[CleanupTaskWeakReference])
                // Synchronize here to avoid being interrupted on stop()
                synchronized {
                    reference.foreach { ref =>
                        referenceBuffer.remove(ref)
                        ref.task.execute(session)
                    }
                }
            } catch {
                case ie: InterruptedException if stopped => // ignore
                case NonFatal(e) => logger.error("Error in cleaning thread", e)
            }
        }
    }
}
