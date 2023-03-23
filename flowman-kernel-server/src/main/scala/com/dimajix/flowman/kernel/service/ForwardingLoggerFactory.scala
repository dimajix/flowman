/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.kernel.service

import java.util.function.Consumer

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

import org.slf4j.ILoggerFactory
import org.slf4j.Logger

import com.dimajix.common.logging.ForwardingEventLogger
import com.dimajix.common.logging.LogEvent
import com.dimajix.flowman.kernel.service.ForwardingLoggerFactory.LogEventConsumer


object ForwardingLoggerFactory {
    class LogEventConsumer(sinks:mutable.Buffer[LogEvent => Unit]) extends Consumer[LogEvent] {
        override def accept(t: LogEvent): Unit = sinks.foreach(_(t))
    }
}
class ForwardingLoggerFactory extends ILoggerFactory {
    private val sinks = mutable.ListBuffer[LogEvent => Unit]()
    private val loggers = TrieMap[String,ForwardingEventLogger]()
    private val consumer = new LogEventConsumer(sinks)

    def addSink(sink:LogEvent => Unit) : Unit = {
        sinks.synchronized {
            sinks += sink
        }
    }

    override def getLogger(name: String): Logger = {
        loggers.getOrElseUpdate(name, new ForwardingEventLogger(name, consumer))
    }
}
