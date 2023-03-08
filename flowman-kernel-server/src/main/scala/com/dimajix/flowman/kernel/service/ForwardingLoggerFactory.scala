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

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

import org.slf4j.ILoggerFactory
import org.slf4j.Logger

import com.dimajix.common.logging.ForwardingEventLogger
import com.dimajix.common.logging.LogEvent


class ForwardingLoggerFactory extends ILoggerFactory {
    private val sinks = mutable.ListBuffer[LogEvent => Unit]()
    private val loggers = TrieMap[String,ForwardingEventLogger]()

    def addSink(sink:LogEvent => Unit) : Unit = {
        sinks.synchronized {
            sinks += sink
        }
    }

    override def getLogger(name: String): Logger = {
        loggers.getOrElseUpdate(name, {
            new ForwardingEventLogger(name, ev => sinks.foreach(_(ev)))
        })
    }
}
