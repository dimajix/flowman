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

package com.dimajix.flowman.tools.rexec.target;

import lombok.val;

import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.model.Status;
import com.dimajix.flowman.kernel.model.TargetIdentifier;
import com.dimajix.flowman.tools.rexec.Command;


class ListCommand extends Command {
    @Override
    public Status execute(KernelClient kernel, SessionClient session) {
        val targets = session.listTargets();
        targets.stream().map(TargetIdentifier::getName).sorted().forEach(System.out::println);
        return Status.SUCCESS;
    }
}
