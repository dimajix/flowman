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

import scala.concurrent.ExecutionContext
import scala.concurrent.Future


object Activity {
    def run(name:String, description:Option[String]=None)(f: => Unit)(implicit executor: ExecutionContext): Activity = {
        AsyncActivity(
            name = name,
            description = description,
            Future(f)
        )
    }
}
trait Activity {
    /**
     * Returns the user-specified name of the activity, or [None] if not specified.
     * @return
     */
    def name: String

    /**
     * Returns the user-specified description of the activity, or [None] if not specified.
     * @return
     */
    def description: Option[String]

    /**
     * Returns `true` if this activity is actively running.
     *
     */
    def isActive: Boolean

    /**
     * Returns the [[OperationException]] if the query was terminated by an exception.
     */
    def exception: Option[OperationException]

    /**
     * Waits for the termination of `this` activity, either by `query.activity()` or by an exception.
     * If the activity has terminated with an exception, then the exception will be thrown.
     *
     * If the query has terminated, then all subsequent calls to this method will either return
     * immediately (if the query was terminated by `stop()`), or throw the exception
     * immediately (if the query has terminated with exception).
     *
     * @throws OperationException if the query has terminated with an exception.
     */
    @throws[OperationException]
    def awaitTermination(): Unit

    /**
     * Waits for the termination of `this` activity, either by `activity.stop()` or by an exception.
     * If the activity has terminated with an exception, then the exception will be thrown.
     * Otherwise, it returns whether the activity has terminated or not within the `timeoutMs`
     * milliseconds.
     *
     * If the activity has terminated, then all subsequent calls to this method will either return
     * `true` immediately (if the query was terminated by `stop()`), or throw the exception
     * immediately (if the activity has terminated with exception).
     *
     * @throws OperationException if the query has terminated with an exception
     */
    @throws[OperationException]
    def awaitTermination(timeoutMs: Long): Boolean

    /**
     * Blocks until all available data in the source has been processed and committed to the sink.
     * This method is intended for testing. Note that in the case of continually arriving data, this
     * method may block forever.
     */
    def processAllAvailable(): Unit

    /**
     * Stops the execution of this activity if it is running. This waits until the termination of the
     * activity execution threads or until a timeout is hit.
     */
    @throws[TimeoutException]
    def stop(): Unit

    def addListener(listener:ActivityListener) : Unit
    def removeListener(listener:ActivityListener) : Unit
}


abstract class AbstractActivity extends Activity {
    protected val listeners = new ActivityListenerBus()

    def addListener(listener:ActivityListener) : Unit = {
        listeners.addListener(listener)
    }
    def removeListener(listener:ActivityListener) : Unit = {
        listeners.removeListener(listener)
    }
}
