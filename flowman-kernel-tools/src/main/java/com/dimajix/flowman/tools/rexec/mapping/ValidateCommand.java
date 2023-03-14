/*
 * Copyright (C) 2023 The Flowman Authors
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

import java.util.Arrays;
import java.util.stream.Collectors;

import lombok.val;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.dimajix.common.ExceptionUtils.isFatal;
import static com.dimajix.common.ExceptionUtils.reasons;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.MappingIdentifier;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.rexec.Command;


public class ValidateCommand extends Command {
    private final Logger logger = LoggerFactory.getLogger(ValidateCommand.class);

    @Argument(usage = "specifies mappings to validate", metaVar = "<mapping>")
    String[] mappings = new String[0];

    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        val mappingNames = mappings.length > 0 ? Arrays.stream(mappings).map(MappingIdentifier::ofString).collect(Collectors.toList()) : session.listMappings();
        logger.info("Validating mappings " + mappingNames.stream().map(MappingIdentifier::toString).reduce((l,r) -> l + "," + r));

        var error = false;
        for (val mapping : mappingNames) {
            try {
                session.validateMapping(mapping);
            } catch (Throwable e) {
                if (isFatal(e))
                    throw e;
                logger.error("Error validating mapping '" + mapping.toString() + "':\n  " + reasons(e));
                error = true;
            }
        }
        return error ? Status.FAILED : Status.SUCCESS;
    }
}
