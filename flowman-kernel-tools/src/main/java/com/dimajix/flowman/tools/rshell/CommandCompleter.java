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

package com.dimajix.flowman.tools.rshell;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import lombok.val;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;

import com.dimajix.flowman.kernel.model.JobIdentifier;
import com.dimajix.flowman.kernel.model.MappingIdentifier;
import com.dimajix.flowman.kernel.model.RelationIdentifier;
import com.dimajix.flowman.kernel.model.TargetIdentifier;
import com.dimajix.flowman.kernel.model.TestIdentifier;


class CommandCompleter implements Completer {
    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        val cmd = new ParsedCommand();
        val parser = new CmdLineParser(cmd);
        val words = line.words().stream().filter(w -> !w.trim().isEmpty()).collect(Collectors.toList());
        List<String> parts = words.isEmpty() ? Collections.emptyList() : words;
        val current = line.word();

        //println(s"parts: ${parts.asScala.mkString(",")}")
        //println(s"word: '$current'")

        try {
            parser.parseArgument(parts);
        }
        catch (CmdLineException e) {
            val eparser = e.getParser();
            val args = eparser.getArguments();
            val opts = eparser.getOptions();
            val all = new LinkedList<OptionHandler>();
            all.addAll(args);
            all.addAll(opts);
            val commands = all.stream().flatMap(opt ->
                Arrays.stream(opt.setter.asAnnotatedElement().getAnnotations()).flatMap(s ->
                    s instanceof SubCommands ?
                        Arrays.stream(((SubCommands)s).value()).map(SubCommand::name)
                    : s instanceof Option ?
                        concat(((Option)s).name(), ((Option)s).aliases())
                    : s instanceof Argument ?
                        argument((Argument) s, opt)
                    :
                        Stream.of()
                )
            );

            commands
                .filter(c -> c.startsWith(current))
                .forEach(c -> candidates.add(new Candidate(c)));
        }
    }

    private static Stream<String> concat(String first, String[] last) {
        return Stream.concat(Stream.of(first), Arrays.stream(last));
    }

    private static Stream<String> argument(Argument arg, OptionHandler opt) {
        if (arg.metaVar().equals("<mapping>"))
            return Shell.getInstance().getSession().listMappings()
                .stream()
                .map(MappingIdentifier::getName)
                .sorted();
        else if (arg.metaVar().equals("<job>"))
            return Shell.getInstance().getSession().listJobs()
                .stream()
                .map(JobIdentifier::getName)
                .sorted();
        else if (arg.metaVar().equals("<test>"))
            return Shell.getInstance().getSession().listTests()
                .stream()
                .map(TestIdentifier::getName)
                .sorted();
        else if (arg.metaVar().equals("<target>"))
            return Shell.getInstance().getSession().listTargets()
                .stream()
                .map(TargetIdentifier::getName)
                .sorted();
        else if (arg.metaVar().equals("<relation>"))
            return Shell.getInstance().getSession().listRelations()
                .stream()
                .map(RelationIdentifier::getName)
                .sorted();
        else if (opt.option.handler() != SubCommandHandler.class && !arg.metaVar().isEmpty())
            return Collections.singletonList(arg.metaVar()).stream();
        else
            return Stream.of();
    }
}
