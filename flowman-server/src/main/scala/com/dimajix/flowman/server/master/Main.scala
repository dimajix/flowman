/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.server.master

import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory


object Main {
    def main(args: Array[String]) : Unit = {
        val options = new Arguments(args)
        val server = new Main(options)

        val result = server.run()
        System.exit(if (result) 0 else 1)
    }
}


class Main(args:Arguments) {
    private val logger = LoggerFactory.getLogger(classOf[Main])

    def run(): Boolean = {
        val log4j = System.getProperty("log4j.configuration")
        if (log4j == null || log4j.isEmpty) {
            val loader = Thread.currentThread.getContextClassLoader
            val url = loader.getResource("com/dimajix/flowman/server/log4j-defaults.properties")
            PropertyConfigurator.configure(url)
            logger.debug(s"Loaded Logging configuration from $url")
        }

        val server = new Server(args)
        server.run()
    }
}
