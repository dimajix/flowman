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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.stream.Collectors;

import dev.dirs.ProjectDirectories;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.val;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.TerminalBuilder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import static com.dimajix.common.ExceptionUtils.reasons;
import static com.dimajix.flowman.common.ConsoleColors.yellow;
import static com.dimajix.flowman.common.ParserUtils.splitSettings;
import static com.dimajix.flowman.tools.Versions.FLOWMAN_VERSION;
import static com.dimajix.flowman.tools.Versions.JAVA_VERSION;

import com.dimajix.flowman.common.Logging;
import com.dimajix.flowman.tools.RemoteTool;


public class Shell extends RemoteTool {
    public static void main(String[] args) {
        Logging.init();

        try {
            val result = run(args);
            if (result)
                System.exit(0);
            else
                System.exit(3);
        }
        catch (CmdLineException ex) {
            System.err.println(ex.getMessage());
            ex.getParser().printUsage(System.err);
            System.err.println();
            System.exit(4);
        }
        catch (Throwable ex) {
            ex.printStackTrace(System.err);
            System.exit(5);
        }
    }

    public static boolean run(String[] args) throws CmdLineException, URISyntaxException, IOException {
        val options = new Arguments(args);
        // Check if only help or version is requested
        if (options.version) {
            System.out.println("Flowman " + FLOWMAN_VERSION);
            System.out.println("Java version " + JAVA_VERSION);
            return true;
        }
        else if (options.isHelp()) {
            options.printHelp(System.out);
            return true;
        }
        else {
            if (options.infoLogging) {
                Logging.setLogging("INFO");
            }
            if (options.debugLogging) {
                Logging.setLogging("DEBUG");
            }

            _instance = new Shell(options);

            // Upload project
            val ws = _instance.getWorkspace();
            ws.uploadWorkspace(new File("."));

            _instance.newSession(options.project);
            _instance.run();

            return true;
        }
    }

    private static Shell _instance = null;
    public static Shell getInstance() { return _instance; }

    private final File historyFile = new File(
        ProjectDirectories.from("com", "dimajix", "Flowman").dataDir, "shell-history");
    private boolean shouldExit = false;


    public Shell(Arguments args) throws URISyntaxException {
        super(new URI(args.kernel), splitSettings(args.config), splitSettings(args.environment), Arrays.asList(args.profiles));
    }


    /**
     * Main method for running this command
     * @return
     */
    public boolean run() throws IOException {
        val terminal = TerminalBuilder.builder()
            .build();
        val console = LineReaderBuilder.builder()
            .appName("Flowman")
            .option(LineReader.Option.CASE_INSENSITIVE, false)
            .option(LineReader.Option.AUTO_MENU, true)
            .option(LineReader.Option.AUTO_LIST, true)
            .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
            .variable(LineReader.HISTORY_FILE, historyFile.toString())
            .terminal(terminal)
            .completer(new CommandCompleter())
            .history(new DefaultHistory())
            .build();
        val writer = terminal.writer();

        console.getHistory().load();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() {
                try {
                    console.getHistory().save();
                }
                catch(IOException ex) {
                    // Warning
                }
            }
        });

        val logo =
            " ______  _\n" +
            "||  ___|| |\n" +
            "|| |_   | |  ___ __      __ _ __ ___    __ _  _ __\n" +
            "||  _|  | | / _ \\\\ \\ /\\ / /| '_ ` _ \\  / _` || '_ \\\n" +
            "|| |    | || (_) |\\ V  V / | | | | | || (_| || | | |\n" +
            "|\\_|    |_| \\___/  \\_/\\_/  |_| |_| |_| \\__,_||_| |_|";

        val kernelInfo = getKernel().getKernel();
        val kernelVersion = kernelInfo.getFlowmanVersion();
        val sparkVersion = kernelInfo.getSparkVersion();
        val hadoopVersion = kernelInfo.getHadoopVersion();
        val scalaVersion = kernelInfo.getScalaVersion();
        val javaVersion = kernelInfo.getJavaVersion();
        val sparkBuildVersion = kernelInfo.getSparkBuildVersion();
        val hadoopBuildVersion = kernelInfo.getHadoopBuildVersion();

        writer.println("\nWelcome to");
        writer.println(logo + "    client: " + FLOWMAN_VERSION + " / kernel: " + kernelVersion + "\n");
        writer.println("Kernel is using Spark " + sparkVersion + " and Hadoop " + hadoopVersion + " and Scala " + scalaVersion + " (Java " + javaVersion+ ")");
        if (!sparkVersion.equals(sparkBuildVersion) || !hadoopVersion.equals(hadoopBuildVersion)) {
            writer.println(yellow("Detected version mismatch in kernel between build and execution environment:"));
            writer.println(yellow("  Hadoop build version: " + hadoopBuildVersion + ", Hadoop execution version: " + hadoopVersion));
            writer.println(yellow("  Spark build version: " + sparkBuildVersion + ", Spark execution version: " + hadoopVersion));
            writer.println(yellow("It is highly recommended to use matching versions, specifically for Spark."));
        }
        writer.println("\nType in 'help' for getting help");

        // REPL-loop
        while (!shouldExit) {
            val cmd = new ParsedCommand();
            try {
                System.err.flush();
                System.out.flush();
                val prompt = "flowman:" + this.getContext() + "> ";

                console.readLine(prompt);
                val args = console.getParsedLine().words().stream().filter(w -> !w.trim().isEmpty()).collect(Collectors.toList());
                if (!args.isEmpty()) {
                    val parser = new CmdLineParser(cmd);
                    parser.parseArgument(args);
                }
            }
            catch (StatusRuntimeException ex) {
                writer.println("Error when communicating with kernel:\n  " + reasons(ex));
                writer.flush();
                shouldExit = true;
            }
            catch (UserInterruptException ex) {
            }
            catch (CmdLineException e) {
                writer.println("Syntax error: " + e.getMessage());
                e.getParser().printUsage(writer, null);
            }
            catch (Throwable e) {
                writer.println("Error parsing command:\n  " + reasons(e));
            }

            val command = cmd.command;
            if (command != null) {
                try {
                    if (command.isHelp()) {
                        command.printHelp(System.out);
                    }
                    else {
                        command.execute(getKernel(), getSession());
                    }
                }
                catch (Exception ex) {
                    writer.println("Error executing command:\n  " + reasons(ex));
                }
            }
        }

        return true;
    }

    public void exit() {
        shouldExit = true;
    }
}
