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

import com.dimajix.flowman.spec.target.Bundle
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spec.task.Job


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
      * @param exec
      * @param phase
      * @param args
      * @param force
      * @return
      */
    def execute(executor: Executor, exec:Bundle, phase:Phase, args:Map[String,String] = Map(), force:Boolean=false) : Status

    /**
      * Creates the container for a single target
      * @param executor
      * @param target the target to create
      */
    def create(executor: Executor, target:Target) : Status

    /**
      * Migrates a single target (if required)
      * @param executor
      * @param target the target to migrate
      */
    def migrate(executor: Executor, target:Target) : Status

    /**
      * Builds a single target
      * @param executor
      * @param target the target to build
      */
    def build(executor: Executor, target:Target, force:Boolean=true) : Status

    /**
      * Truncates the data of a single target
      * @param executor
      * @param target the target to truncate
      */
    def truncate(executor: Executor, target:Target) : Status

    /**
      * Cleans a single target
      * @param executor
      * @param target the target to destroy
      */
    def destroy(executor: Executor, target:Target) : Status
}
