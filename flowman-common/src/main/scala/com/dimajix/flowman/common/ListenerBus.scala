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

package com.dimajix.flowman.common

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory


trait ListenerBus[L <: AnyRef, E] {
    private val logger = LoggerFactory.getLogger(classOf[ListenerBus[L,E]])
    private val listeners = new CopyOnWriteArrayList[L]

    /**
     * Add a listener to listen events. This method is thread-safe and can be called in any thread.
     */
    final def addListener(listener: L): Unit = {
        listeners.add(listener)
    }

    /**
     * Remove a listener and it won't receive any events. This method is thread-safe and can be called
     * in any thread.
     */
    final def removeListener(listener: L): Unit = {
        listeners.asScala.find(_ eq listener).foreach { l =>
            listeners.remove(l)
        }
    }


    /**
     * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
     * `postToAll` in the same thread for all events.
     */
    def postToAll(event: E): Unit = {
        // JavaConverters can create a JIterableWrapper if we use asScala.
        // However, this method will be called frequently. To avoid the wrapper cost, here we use
        // Java Iterator directly.
        val iter = listeners.iterator
        while (iter.hasNext) {
            val listener = iter.next()
            try {
                doPostEvent(listener, event)
                if (Thread.interrupted()) {
                    // We want to throw the InterruptedException right away so we can associate the interrupt
                    // with this listener, as opposed to waiting for a queue.take() etc. to detect it.
                    throw new InterruptedException()
                }
            } catch {
                case ie: InterruptedException =>
                    logger.warn(s"Interrupted while posting to ${getSimpleName(listener.getClass)}. Removing that listener.", ie)
                    removeListener(listener)
                case NonFatal(e) =>
                    logger.warn(s"Listener ${getSimpleName(listener.getClass)} threw an exception", e)
            }
        }
    }

    /**
     * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
     * thread for all listeners.
     */
    protected def doPostEvent(listener: L, event: E): Unit


    /**
     * Safer than Class obj's getSimpleName which may throw Malformed class name error in scala.
     * This method mimicks scalatest's getSimpleNameOfAnObjectsClass.
     */
    private def getSimpleName(cls: Class[_]): String = {
        try {
            cls.getSimpleName
        } catch {
            case _: InternalError => cls.getName
        }
    }
}
