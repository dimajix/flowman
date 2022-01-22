/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.client

import java.io.PrintStream
import java.net.URI

import org.apache.http.impl.client.CloseableHttpClient
import org.kohsuke.args4j.CmdLineParser


abstract class NestedCommand extends Command {
    var command:Command

    /**
      * Returns true if a help message is requested
      * @return
      */
    override def help : Boolean = _help || command == null || (command != null && command.help)

    /**
      * Prints a context-aware help message
      */
    override def printHelp(out:PrintStream = System.err) : Unit = {
        if (command != null) {
            command.printHelp(out)
        }
        else {
            new CmdLineParser(this).printUsage(out)
            out.println
        }
    }


    override def execute(httpClient:CloseableHttpClient, baseUri:URI) : Boolean = {
        command.execute(httpClient, baseUri)
    }
}
