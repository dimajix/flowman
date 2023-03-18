/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.tools.rshell;

import java.io.PrintStream;
import java.util.Arrays;

import lombok.val;
import org.kohsuke.args4j.CmdLineException;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


class Arguments {
    @Option(name = "-h", aliases= {"--help"}, usage = "show help", help=true)
    boolean _help = false;
    @Option(name = "-v", aliases={"--version"}, usage = "show version", help=true)
    boolean version = false;
    @Option(name = "-f", aliases={"--project"}, usage = "project name or project file or directory", metaVar = "<project>")
    String project = "project.yml";
    @Option(name = "-P", aliases={"--profile"}, usage = "activate profile with specified name", metaVar = "<profile>")
    String[] profiles = new String[0];
    @Option(name = "-D", aliases={"--env"}, usage = "set environment variables which can be accessed inside config", metaVar = "<key=value>")
    String[] environment = new String[0];
    @Option(name = "-X", aliases = {"--verbose"}, usage = "Produce execution output at info level.")
    boolean infoLogging = false;
    @Option(name = "-XX", aliases = {"--debug"}, usage = "Produce execution output at debug level.")
    boolean debugLogging = false;
    @Option(name = "-k" , aliases = {"--kernel"}, usage = "URI to Flowman kernel", metaVar = "<uri>")
    String kernel = "grpc://localhost:8088/";
    @Option(name = "--conf", usage = "set a Flowman or Spark config", metaVar = "<key=value>")
    String[] config = new String[0];
    @Option(name = "--info", usage = "dump configuration information")
    boolean info = false;

    public Arguments(String[] args) throws CmdLineException {
        parseArgs(args);
    }

    /**
      * Returns true if a help message is requested
      * @return
      */
    public boolean isHelp() { return _help; }

    /**
      * Prints a context-aware help message
      */
    public void printHelp(PrintStream out) {
        new CmdLineParser(this).printUsage(out);
        out.println();
    }

    private void parseArgs(String[] args) throws CmdLineException {
        val parser = new CmdLineParser(this);
        parser.parseArgument(Arrays.asList(args));
    }
}
