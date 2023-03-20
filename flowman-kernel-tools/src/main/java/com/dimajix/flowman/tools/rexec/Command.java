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

package com.dimajix.flowman.tools.rexec;

import java.io.PrintStream;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;


public abstract class Command {
    @Option(name = "-h", aliases={"--help"}, usage = "show help", help=true)
    public boolean _help = false;

    /**
      * Returns true if the command line is incomplete
      * @return
      */
    public boolean isIncomplete() { return false; }

    /**
      * Returns true if a help message is requested or required
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

    public abstract Status execute(ExecutionContext context);
}
