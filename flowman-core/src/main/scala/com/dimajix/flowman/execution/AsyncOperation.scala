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

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success

import com.dimajix.flowman.execution.OperationListener._


case class AsyncOperation(
    override val name: String,
    override val description: Option[String] = None,
    future:Future[Unit]
) extends AbstractOperation {
    private val promise = Promise[Unit]()
    private val value = promise.future

    future.onComplete { result =>
        // First store result in promise
        result match {
            case Failure(ex: OperationException) =>
                promise.failure(ex)
            case Failure(ex) =>
                promise.failure(new OperationException(ex.getMessage, ex))
            case Success(s) =>
                promise.success(s)
        }
        // Then call all listeners
        listeners.postToAll(OperationTerminatedEvent(this))
    }

    /**
     * Returns `true` if this operation is actively running.
     *
     */
    override def isActive: Boolean = !value.isCompleted

    /**
     * Returns the [[OperationException]] if the query was terminated by an exception.
     */
    override def exception: Option[OperationException] = value.value match {
        case Some(Failure(exception:OperationException)) => Some(exception)
        case Some(Failure(ex:Throwable)) => Some(new OperationException(ex.getMessage, ex)) // Should not happen
        case Some(Success(_)) => None
        case None => None
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
    override def awaitTermination(): Unit = {
        Await.result(value, Duration.Inf)
    }

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
    override def awaitTermination(timeoutMs: Long): Boolean = {
        try {
            Await.result(value, Duration.create(timeoutMs, TimeUnit.MILLISECONDS))
            true
        }
        catch {
            case _:concurrent.TimeoutException => false
        }
    }

    /**
     * Blocks until all available data in the source has been processed and committed to the sink.
     * This method is intended for testing. Note that in the case of continually arriving data, this
     * method may block forever.
     */
    override def processAllAvailable(): Unit = {}

    /**
     * Stops the execution of this operation if it is running. This waits until the termination of the
     * operation execution threads or until a timeout is hit.
     */
    override def stop(): Unit = {
        try {
            awaitTermination()
        }
        catch {
            case _:OperationException =>
        }
    }
}
