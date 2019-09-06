/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.execution

import com.dimajix.flowman.spec.target.Batch
import com.dimajix.flowman.spec.target.Target


/**
  * This class wraps the execution of Jobs and is responsible for appropriate exception handling (for example
  * logging or storing information about failed jobs into a database)
  */
abstract class Runner {
    /**
      * Executes a single job using the given executor and a map of parameters. The Runner may decide not to
      * execute a specific job, because some information may indicate that the job has already been successfully
      * run in the past. This behaviour can be overriden with the force flag
      * @param executor
      * @param batch
      * @param phase
      * @param args
      * @param force
      * @return
      */
    def execute(executor: Executor, batch:Batch, phase:Phase, args:Map[String,String] = Map(), force:Boolean=false) : Status

    /**
      * Executes a single job using the given executor and a map of parameters. The Runner may decide not to
      * execute a specific job, because some information may indicate that the job has already been successfully
      * run in the past. This behaviour can be overriden with the force flag
      * @param executor
      * @param target
      * @param phase
      * @param force
      * @return
      */
    def execute(executor: Executor, target:Target, phase:Phase, force:Boolean=false) : Status
}
