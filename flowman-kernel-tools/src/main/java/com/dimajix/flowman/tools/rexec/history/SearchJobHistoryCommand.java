/*
 * Copyright (C) 2020 The Flowman Authors
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

package com.dimajix.flowman.tools.rexec.history;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.val;
import org.kohsuke.args4j.Option;

import com.dimajix.flowman.kernel.ConsoleUtils;
import com.dimajix.flowman.kernel.model.JobOrder;
import com.dimajix.flowman.kernel.model.JobQuery;
import com.dimajix.flowman.kernel.model.JobState;
import com.dimajix.flowman.kernel.model.Phase;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;
import static com.dimajix.flowman.common.ParserUtils.splitSettings;


public class SearchJobHistoryCommand extends Command {
    @Option(name = "-P", aliases={"--project"}, usage = "name of project", metaVar = "<project>")
    String project = "";
    @Option(name = "-j", aliases={"--job"}, usage = "name of job", metaVar = "<job>")
    String job = "";
    @Option(name = "-s", aliases={"--status"}, usage = "status of job (UNKNOWN, RUNNING, SUCCESS, FAILED, ABORTED, SKIPPED)", metaVar = "<status>")
    String status = "";
    @Option(name = "-p", aliases={"--phase"}, usage = "execution phase (CREATE, BUILD, VERIFY, TRUNCATE, DESTROY)", metaVar = "<phase>")
    String phase = "";
    @Option(name = "-a", aliases={"--arg"}, usage = "job argument (key=value)", metaVar = "<phase>")
    String[] args = new String[0];
    @Option(name = "-n", aliases={"--limit"}, usage = "maximum number of results", metaVar = "<limit>")
    int limit = 100;

    @Override
    public Status execute(ExecutionContext context) {
        val history = context.getHistory();
        val kernel = context.getKernel();
        val namespace = kernel.getNamespace().getName();
        val query = new JobQuery(
            Collections.emptyList(),
            Collections.singletonList(namespace),
            split(project),
            split(job),
            split(status).stream().map(Status::ofString).collect(Collectors.toList()),
            split(phase).stream().map(Phase::ofString).collect(Collectors.toList()),
            splitSettings(args),
            Optional.empty(),
            Optional.empty()
        );

        val jobs = history.findJobs(query, Collections.singletonList(JobOrder.BY_DATETIME), limit);
        val header = new String[]{"id", "namespace", "project", "version", "job", "arguments", "phase", "status", "start", "end", "error"};
        val rows = jobs.stream().map(SearchJobHistoryCommand::toRow).collect(Collectors.toList());
        ConsoleUtils.showTable(header, rows);
        return Status.SUCCESS;
    }

    private static String[] toRow(JobState state) {
        return new String[]{
            state.getId(),
            state.getNamespace(),
            state.getProject(),
            state.getVersion(),
            state.getJob(),
            state.getArguments().entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).reduce((l, r) -> l + "," + r).orElse(""),
            state.getPhase().toString(),
            state.getStatus().toString(),
            state.getStartDateTime().map(ZonedDateTime::toString).orElse(""),
            state.getEndDateTime().map(ZonedDateTime::toString).orElse(""),
            state.getError().orElse("")
        };
    }

    private static List<String> split(String arg) {
        return Arrays.stream(arg.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }
}
