/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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


sealed abstract class MigrationPolicy extends Product with Serializable
object MigrationPolicy {
    case object RELAXED extends MigrationPolicy
    case object STRICT extends MigrationPolicy

    def ofString(mode:String) : MigrationPolicy = {
        mode.toLowerCase(Locale.ROOT) match {
            case "relaxed" => MigrationPolicy.RELAXED
            case "strict" => MigrationPolicy.STRICT
            case _ => throw new IllegalArgumentException(s"Unknown migration policy: '$mode'. " +
                "Accepted migration policies are 'relaxed' and 'strict'.")
        }
    }
}


sealed abstract class MigrationStrategy extends Product with Serializable
object MigrationStrategy {
    case object NEVER extends MigrationStrategy
    case object FAIL extends MigrationStrategy
    case object ALTER extends MigrationStrategy
    case object ALTER_REPLACE extends MigrationStrategy
    case object REPLACE extends MigrationStrategy

    def ofString(mode:String) : MigrationStrategy = {
        mode.toLowerCase(Locale.ROOT) match {
            case "never" => MigrationStrategy.NEVER
            case "fail" => MigrationStrategy.FAIL
            case "alter" => MigrationStrategy.ALTER
            case "alter_replace"|"alterreplace" => MigrationStrategy.ALTER_REPLACE
            case "replace" => MigrationStrategy.REPLACE
            case _ => throw new IllegalArgumentException(s"Unknown migration strategy: '$mode'. " +
                "Accepted migration strategy are 'NEVER', 'FAIL', 'ALTER', 'ALTER_REPLACE' and 'REPLACE'.")
        }
    }
}
