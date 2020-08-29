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

import java.util

import scala.collection.JavaConverters._

import jline.console.completer.Completer
import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.spi.SubCommandHandler
import org.kohsuke.args4j.spi.SubCommands
import org.kohsuke.args4j.Option


class CommandCompleter extends Completer {
    override def complete(buffer: String, cursor: Int, candidates: util.List[CharSequence]): Int = {
        val cmd = new ParsedCommand
        val parser = new CmdLineParser(cmd)
        val parts = buffer.split(' ')
        val current = parts.lastOption.getOrElse("")
        try {
            parser.parseArgument(parts.toList.asJava)
            buffer.length
        }
        catch {
            case e: CmdLineException =>
                val parser = e.getParser
                val args = parser.getArguments.asScala
                val opts = parser.getOptions.asScala
                val SCH = classOf[SubCommandHandler]
                val commands = (args ++ opts).flatMap { opt =>
                    opt.setter.asAnnotatedElement.getAnnotations.flatMap {
                        case cmd: SubCommands =>
                            cmd.value().map(_.name())
                        case o:Option =>
                            Seq(o.name()) ++ o.aliases()
                        case a:Argument =>
                            Seq(a.metaVar())
                        case _ =>
                            Seq()
                    }
                }
                commands.filter(_.startsWith(current)).foreach(candidates.add)
                //parts.dropRight(1).mkString(" ").length
                buffer.length
        }
    }
}
