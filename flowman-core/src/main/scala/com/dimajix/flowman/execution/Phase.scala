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

import java.util.Locale

import com.dimajix.flowman.spec.target.Target

sealed abstract class Phase  {
    val value:String

    def execute(executor: Executor, target:Target) : Unit
    def execute(runner: Runner, executor: Executor, target:Target) : Status

    override def toString: String = value
}

object Phase {
    case object CREATE extends Phase {
        override val value = "create"
        override def execute(executor: Executor, target: Target): Unit = target.create(executor)
        override def execute(runner: Runner, executor: Executor, target: Target): Status = runner.create(executor, target)
    }
    case object MIGRATE extends Phase {
        override val value = "migrate"
        override def execute(executor: Executor, target: Target): Unit = target.migrate(executor)
        override def execute(runner: Runner, executor: Executor, target: Target): Status = runner.migrate(executor, target)
    }
    case object BUILD extends Phase {
        override val value = "build"
        override def execute(executor: Executor, target: Target): Unit = target.build(executor)
        override def execute(runner: Runner, executor: Executor, target: Target): Status = runner.build(executor, target)
    }
    case object TRUNCATE extends Phase {
        override val value = "truncate"
        override def execute(executor: Executor, target: Target): Unit = target.truncate(executor)
        override def execute(runner: Runner, executor: Executor, target: Target): Status = runner.truncate(executor, target)
    }
    case object DESTROY extends Phase {
        override val value = "destroy"
        override def execute(executor: Executor, target: Target): Unit = target.destroy(executor)
        override def execute(runner: Runner, executor: Executor, target: Target): Status = runner.destroy(executor, target)
    }

    def ofString(status:String) : Phase = {
        status.toLowerCase(Locale.ROOT) match {
            case CREATE.value => CREATE
            case MIGRATE.value => MIGRATE
            case BUILD.value => BUILD
            case TRUNCATE.value => TRUNCATE
            case DESTROY.value => DESTROY
            case _ => throw new IllegalArgumentException(s"No phase defined for '$status'")
        }
    }
}
