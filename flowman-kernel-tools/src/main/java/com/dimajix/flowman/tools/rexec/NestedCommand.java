/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.Status;


public abstract class NestedCommand extends Command {
    public abstract Command getCommand();

    /**
      * Returns true if a help message is requested
      * @return
      */
    @Override
    public boolean isHelp() { return _help || getCommand() == null || (getCommand() != null && getCommand().isHelp()); }

    /**
      * Prints a context-aware help message
      */
    public void printHelp(PrintStream out) {
        if (getCommand() != null) {
            getCommand().printHelp(out);
        }
        else {
            new CmdLineParser(this).printUsage(out);
            out.println();
        }
    }


    public Status execute(KernelClient kernel, SessionClient session) {
        return getCommand().execute(kernel, session);
    }
}
