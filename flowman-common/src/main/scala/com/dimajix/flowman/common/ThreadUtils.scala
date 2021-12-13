/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import java.lang.Thread.UncaughtExceptionHandler

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.forkjoin.ForkJoinPool
import scala.concurrent.forkjoin.ForkJoinWorkerThread

import org.slf4j.LoggerFactory


class ThreadUtils
object ThreadUtils {
    private val logger = LoggerFactory.getLogger(classOf[ThreadUtils])

    private class MyForkJoinWorkerThread(prefix:String, pool:ForkJoinPool) extends ForkJoinWorkerThread(pool) { // set the correct classloader here
        setContextClassLoader(Thread.currentThread.getContextClassLoader)
        setName(prefix + "-" + super.getName)
    }
    private val exceptionHandler = new UncaughtExceptionHandler {
        override def uncaughtException(thread: Thread, throwable: Throwable): Unit = {
            logger.error("Uncaught exception: ", throwable)
        }
    }

    /**
     * Creates a new ForkJoinPool
     * @param maxThreadNumber
     * @return
     */
    def newThreadPool(prefix:String, maxThreadNumber:Int): ForkJoinPool = {
        val factory = new ForkJoinPool.ForkJoinWorkerThreadFactory {
            override final def newThread(pool: ForkJoinPool) = {
                new MyForkJoinWorkerThread(prefix, pool)
            }
        }
        new ForkJoinPool(
            maxThreadNumber,
            factory,
            exceptionHandler,
            true
        )
    }

    /**
     * Transforms input collection by applying the given function to each element in parallel fashion.
     *
     * @param in - the input collection which should be transformed in parallel.
     * @param prefix - the prefix assigned to the underlying thread pool.
     * @param maxThreads - maximum number of thread can be created during execution.
     * @param f - the lambda function will be applied to each element of `in`.
     * @tparam I - the type of elements in the input collection.
     * @tparam O - the type of elements in resulted collection.
     * @return new collection in which each element was given from the input collection `in` by
     *         applying the lambda function `f`.
     */
    def parmap[I, O](in: Seq[I], prefix: String, maxThreads: Int)(f: I => O): Seq[O] = {
        val pool = newThreadPool(prefix, maxThreads)
        try {
            implicit val ec = ExecutionContext.fromExecutor(pool)

            val futures = in.map(x => Future(f(x)))
            val futureSeq = Future.sequence(futures)

            Await.result(futureSeq, Duration.Inf)
        } finally {
            pool.shutdownNow()
        }
    }
}
