/*
 * Copyright 2021 Kaya Kupferschmidt
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

import java.util.concurrent.TimeoutException

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.streaming.StreamingQueryListener


case class StreamingOperation(
    override val name:String,
    query:StreamingQuery
) extends AbstractOperation {
    import OperationListener._

    @volatile
    private var streamDeathCause: OperationException = null

    // Register Spark listener, so we can forward the termination of the query
    private val listener = new StreamingQueryListener {
        override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        }
        override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        }
        override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
            listeners.postToAll(OperationTerminatedEvent(StreamingOperation.this))
            query.sparkSession.streams.removeListener(this)
        }
    }
    query.sparkSession.streams.addListener(listener)

    /**
     * Returns the user-specified description of the operation, or [None] if not specified.
     *
     * @return
     */
    override def description: Option[String] = Option(query.name)

    /**
     * Returns `true` if this operation is actively running.
     *
     */
    override def isActive: Boolean = query.isActive

    /**
     * Returns the [[OperationException]] if the query was terminated by an exception.
     */
    override def exception: Option[OperationException] = {
        if (streamDeathCause == null) {
            query.exception.foreach(storeException)
        }
        Option(streamDeathCause)
    }

    /**
     * Waits for the termination of `this` operation, either by `query.operation()` or by an exception.
     * If the operation has terminated with an exception, then the exception will be thrown.
     *
     * If the query has terminated, then all subsequent calls to this method will either return
     * immediately (if the query was terminated by `stop()`), or throw the exception
     * immediately (if the query has terminated with exception).
     *
     * @throws OperationException if the query has terminated with an exception.
     */
    @throws[OperationException]
    override def awaitTermination(): Unit = recordException(query.awaitTermination())

    /**
     * Waits for the termination of `this` operation, either by `operation.stop()` or by an exception.
     * If the operation has terminated with an exception, then the exception will be thrown.
     * Otherwise, it returns whether the operation has terminated or not within the `timeoutMs`
     * milliseconds.
     *
     * If the operation has terminated, then all subsequent calls to this method will either return
     * `true` immediately (if the query was terminated by `stop()`), or throw the exception
     * immediately (if the operation has terminated with exception).
     *
     * @throws OperationException if the query has terminated with an exception
     */
    @throws[OperationException]
    override def awaitTermination(timeoutMs: Long): Boolean = recordException(query.awaitTermination(timeoutMs))

    /**
     * Blocks until all available data in the source has been processed and committed to the sink.
     * This method is intended for testing. Note that in the case of continually arriving data, this
     * method may block forever.
     */
    override def processAllAvailable(): Unit = query.processAllAvailable()

    /**
     * Stops the execution of this operation if it is running. This waits until the termination of the
     * operation execution threads or until a timeout is hit.
     */
    @throws[TimeoutException]
    override def stop(): Unit = query.stop()

    @throws[OperationException]
    private def recordException[T](f: => T) : T = {
        try {
            f
        }
        catch {
            case ex:StreamingQueryException =>
                storeException(ex)
                throw streamDeathCause
        }
    }

    private def storeException(ex:StreamingQueryException) : Unit = {
        synchronized {
            if (streamDeathCause == null) {
                streamDeathCause = new OperationException("Underlying Spark stream threw an exception", ex)
            }
        }
    }
}
