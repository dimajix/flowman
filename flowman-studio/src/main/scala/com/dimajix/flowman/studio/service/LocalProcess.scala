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

package com.dimajix.flowman.studio.service

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.sys.process.ProcessBuilder
import scala.sys.process.ProcessLogger
import scala.util.Try

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory


class LocalProcess(builder:ProcessBuilder, system:ActorSystem) extends Process {
    private val logger = LoggerFactory.getLogger(classOf[LocalProcess])
    private implicit val as: ActorSystem = system
    private implicit val ec: ExecutionContext = system.dispatcher
    private implicit val materializer: ActorMaterializer = ActorMaterializer()

    private var terminated = false
    private val (loggerQueue, loggerPublisher) = Source
        .queue[String](1000, OverflowStrategy.dropHead)
        .toMat(Sink.asPublisher(fanout = true))(Keep.both)
        .run()
    private val processLogger = new ProcessLogger {
        override def out(s: => String): Unit = {
            logger.info("stdout: " + s)
            loggerQueue.offer(s)
        }
        override def err(s: => String): Unit = {
            logger.info("stderr: " + s)
            loggerQueue.offer(s)
        }
        override def buffer[T](f: => T): T = f
    }
    private val process = builder.run(processLogger, connectInput = false)
    private val exitValue = Future {
        val result = process.exitValue()
        loggerQueue.complete()
        terminated = true
        result
    }

    /**
     * Tries to shutdown the process
     */
    override def shutdown(): Unit = {
        if (!terminated) {
            terminated = true
            process.destroy()
        }
    }

    /**
     * Returns the current state of the process
     * @return
     */
    override def state : ProcessState = {
        // We cannot use process.isAlive, since this method is not available in Scala 2.11
        if (exitValue.isCompleted) {
            if (!terminated)
                ProcessState.STARTING
            else
                ProcessState.TERMINATED
        } else {
            if (terminated)
                ProcessState.STOPPING
            else
                ProcessState.RUNNING
        }
    }

    /**
     * Returns a publisher for all console messages produced by the process
     * @return
     */
    override def messages : Publisher[String] = loggerPublisher
}
