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

package com.dimajix.flowman.tools.shell

import java.io.File

import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import dev.dirs.ProjectDirectories
import org.apache.hadoop.fs.Path
import org.jline.reader.LineReader
import org.jline.reader.LineReaderBuilder
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.TerminalBuilder
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser

import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.tools.Logging
import com.dimajix.flowman.tools.StatefulTool
import com.dimajix.flowman.util.withShutdownHook


object Shell {
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

            _instance = new Shell(options)
            _instance.loadProject(new Path(options.projectFile))
            _instance.run()
        }
    }

    var _instance:Shell = _
    def instance:Shell = _instance
}



class Shell(args:Arguments) extends StatefulTool(
    config = splitSettings(args.config).toMap,
    environment = splitSettings(args.environment).toMap,
    args.profiles.toSeq,
    args.sparkMaster,
    args.sparkName
) {
    val historyFile = new File(
        ProjectDirectories.from("com", "dimajix", "Flowman").dataDir,
        "shell-history")
    /**
     * Main method for running this command
     * @return
     */
    def run() : Boolean = {
        val terminal = TerminalBuilder.builder()
            .build()
        val console = LineReaderBuilder.builder()
            .appName("Flowman")
            .option(LineReader.Option.CASE_INSENSITIVE, false)
            .option(LineReader.Option.AUTO_MENU, true)
            .option(LineReader.Option.AUTO_LIST, true)
            .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
            .variable(LineReader.HISTORY_FILE, historyFile.toString)
            .terminal(terminal)
            .completer(new CommandCompleter)
            .history(new DefaultHistory)
            .build()
        val writer = terminal.writer()

        console.getHistory.load()
        Runtime.getRuntime.addShutdownHook(new Thread() { override def run() : Unit = console.getHistory.save() })

        // REPL-loop
        while (true) {
            val cmd = new ParsedCommand
            try {
                System.err.flush()
                System.out.flush()
                val context = job.map(_.name).orElse(test.map(_.name))
                val prompt = "flowman:" + project.name + context.map("/" + _).getOrElse("") + "> "
                console.readLine(prompt)
                val args = console.getParsedLine.words().asScala.filter(_.trim.nonEmpty)
                if (args.nonEmpty) {
                    val parser = new CmdLineParser(cmd)
                    parser.parseArgument(args.asJava)
                }
            } catch {
                case e: CmdLineException =>
                    writer.println("Syntax error: " + e.getMessage)
                    e.getParser.printUsage(writer, null)
                case NonFatal(e) =>
                    writer.println("Error parsing command: " + e.getMessage)
            }

            try {
                val command = cmd.command
                if (command != null) {
                    if (command.help) {
                        command.printHelp(System.out)
                    }
                    else {
                        command.execute(session, project, context)
                    }
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
