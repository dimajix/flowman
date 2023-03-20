/*
 * Copyright (C) 2019 The Flowman Authors
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

package com.dimajix.flowman.tools.rexec.mapping;

import lombok.val;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.dimajix.flowman.kernel.model.MappingOutputIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.ExecutionContext;
import com.dimajix.flowman.tools.rexec.Command;
import static com.dimajix.flowman.common.ParserUtils.splitSettings;


class SaveCommand extends Command {
    @Option(name="-f", aliases={"--format"}, usage="Specifies the format", metaVar="<format>", required = false)
    String format = "csv";
    @Option(name="-o", aliases={"--option"}, usage = "additional format specific option", metaVar = "<key=value>", required = false)
    String[] options = new String[0];
    @Argument(usage = "specifies the mapping to save", metaVar = "<mapping>", required = true, index = 0)
    String mapping = "";
    @Argument(usage = "specifies the output filename", metaVar = "<filename>", required = true, index = 1)
    String location = "";

    @Override
    public Status execute(ExecutionContext context) {
        val session = context.getSession();
        val mappingId = MappingOutputIdentifier.ofString(mapping);
        return session.saveMapping(mappingId, location, format, splitSettings(options));
    }
}
