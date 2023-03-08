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

package com.dimajix.flowman.tools.rexec.namespace;

import java.util.Map;

import lombok.val;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.tools.rexec.Command;


public class InspectCommand extends Command {
    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        val namespace = kernel.getNamespace();
        System.out.println("Namespace:");
        System.out.println("    name: " + namespace.getName());
        System.out.println("    plugins: " + namespace.getPlugins().stream().reduce((k,v) -> k + "," + v));

        System.out.println("Environment:");
        namespace.getEnvironment()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Configuration:");
        namespace.getConfig()
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(kv -> System.out.println("    " + kv.getKey() + "=" + kv.getValue()));

        System.out.println("Profiles:");
        namespace.getProfiles()
            .stream()
            .sorted()
            .forEach(p -> System.out.println("    " + p));

        System.out.println("Connections:");
        namespace.getConnections()
            .stream()
            .sorted()
            .forEach(p -> System.out.println("    " + p));

        return Status.SUCCESS;
    }
}
