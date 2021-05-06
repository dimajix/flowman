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

import scala.sys.process.ProcessBuilder
import scala.sys.process.ProcessLogger

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.reactivestreams.Publisher


class LocalProcess(builder:ProcessBuilder, system:ActorSystem) extends Process {
    private implicit val ec = system
    private implicit val materializer = ActorMaterializer()

    private var terminated = false
    private val (loggerQueue, loggerPublisher) = Source
        .queue[String](1000, OverflowStrategy.dropHead)
        .toMat(Sink.asPublisher(fanout = true))(Keep.both)
        .run()
    private val logger = new ProcessLogger {
        override def out(s: => String): Unit = loggerQueue.offer(s)
        override def err(s: => String): Unit = loggerQueue.offer(s)
        override def buffer[T](f: => T): T = {
            try {
                f
            } finally {
                loggerQueue.complete()
            }
        }
    }
    private val process = builder.run(logger, connectInput = false)

    override def shutdown(): Unit = {
        if (!terminated) {
            terminated = true
            process.destroy()
        }
    }
    override def state : ProcessState = {
        if (process.isAlive()) {
            if (terminated)
                ProcessState.STOPPING
            else
                ProcessState.RUNNING
        } else {
            ProcessState.TERMINATED
        }
    }
    override def messages : Publisher[String] = loggerPublisher
}
