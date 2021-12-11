/*
 * Copyright 2019-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.testing;

import com.dimajix.flowman.execution.Lifecycle;
import com.dimajix.flowman.execution.Phase;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;


public class JavaRunnerTest {
    @Test
    public void testRunner() {
        Runner runner = Runner.builder()
            .withProfile("test")
            .withProject(Resources.getResource("flows/project.yml"))
            .build();

        boolean result = runner.runJob("main", Collections.singletonList(Phase.ofString("build")), Collections.emptyMap());
        assertThat(result).isTrue();
        result = runner.runJob("main", Lifecycle.DEFAULT(), Collections.emptyMap());
        assertThat(result).isTrue();
    }
}
