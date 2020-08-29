/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.cli

import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.jline.reader.LineReader
import org.jline.reader.LineReaderBuilder
import org.jline.terminal.TerminalBuilder
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.tools.Logging
import com.dimajix.flowman.tools.Tool


object Driver {
    def main(args: Array[String]) : Unit = {
        Logging.init()

        Try {
            run(args:_*)
        }
        match {
            case Success (true) =>
                System.exit(0)
            case Success (false) =>
                System.exit(1)
            case Failure(ex:CmdLineException) =>
                System.err.println(ex.getMessage)
                ex.getParser.printUsage(System.err)
                System.err.println
                System.exit(1)
            case Failure(exception) =>
                exception.printStackTrace(System.err)
                System.exit(1)
        }
    }

    def run(args: String*) : Boolean = {
        val options = new Arguments(args.toArray)
        // Check if only help is requested
        if (options.help) {
            options.printHelp(System.out)
            true
        }
        else {
            Logging.setSparkLogging(options.sparkLogging)

            val driver = new Driver(options)
            driver.run()
        }
    }
}


class Driver(options:Arguments) extends Tool {
    /**
     * Main method for running this command
     * @return
     */
    def run() : Boolean = {
        val project = loadProject(new Path(options.projectFile))

        // Create Flowman Session, which also includes a Spark Session
        val config = splitSettings(options.config)
        val environment = splitSettings(options.environment)
        val session = createSession(
            options.sparkMaster,
            options.sparkName,
            project = Some(project),
            additionalConfigs = config.toMap,
            additionalEnvironment = environment.toMap,
            profiles = options.profiles
        )

        val terminal = TerminalBuilder.builder()
            .build()
        val console = LineReaderBuilder.builder()
            .appName("Flowman")
            .option(LineReader.Option.CASE_INSENSITIVE, false)
            .option(LineReader.Option.AUTO_MENU, true)
            .option(LineReader.Option.AUTO_LIST, true)
            .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
            .terminal(terminal)
            .completer(new CommandCompleter)
            .build()
        val writer = terminal.writer()

        //console.setAutosuggestion(LineReader.SuggestionType.COMPLETER)

        // REPL-loop
        while (true) {
            val cmd = new ParsedCommand
            try {
                System.err.flush()
                System.out.flush()
                console.readLine("flowman> ")
                val line = console.getParsedLine
                if (line.words().asScala.exists(_.trim.nonEmpty)) {
                    val parser = new CmdLineParser(cmd)
                    parser.parseArgument(line.words())
                }
            } catch {
                case e: CmdLineException =>
                    writer.println("Syntax error: " + e.getMessage)
                    e.getParser.printUsage(writer, null)
                case NonFatal(e) =>
                    writer.println("Error parsing command: " + e.getMessage)
            }

            try {
                if (cmd.command != null) {
                    cmd.command.execute(project, session)
                }
            }
            catch {
                case NonFatal(e) =>
                    writer.println("Error executing command: " + e.getMessage)
                    e.printStackTrace(writer)
            }
        }

        true
    }
}
