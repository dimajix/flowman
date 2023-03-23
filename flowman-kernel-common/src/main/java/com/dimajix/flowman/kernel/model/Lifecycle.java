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

package com.dimajix.flowman.kernel.model;

import java.util.Arrays;
import java.util.List;


public class Lifecycle {
    public static final Lifecycle BUILD = new Lifecycle(Arrays.asList(
        Phase.VALIDATE,
        Phase.CREATE,
        Phase.BUILD,
        Phase.VERIFY
    ));
    public static final Lifecycle CLEAN = new Lifecycle(Arrays.asList(Phase.TRUNCATE));
    public static final Lifecycle DESTROY = new Lifecycle(Arrays.asList(Phase.DESTROY));
    public static final Lifecycle ALL = new Lifecycle(Arrays.asList(
        Phase.VALIDATE,
        Phase.CREATE,
        Phase.BUILD,
        Phase.VERIFY,
        Phase.TRUNCATE,
        Phase.DESTROY
    ));


    public final List<Phase> phases;
    private static final List<Lifecycle> all = Arrays.asList(BUILD, CLEAN, DESTROY);

    Lifecycle(List<Phase> phases) {
        this.phases = phases;
    }

    public static Lifecycle ofPhase(Phase phase) {
        return all.stream().map(l -> new Lifecycle(l.phases.subList(0, l.phases.indexOf(phase) + 1)))
            .filter(l -> !l.phases.isEmpty())
            .findFirst()
            .get();
    }
}
