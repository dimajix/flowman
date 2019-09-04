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


sealed abstract class Status {
    val value: String

    override def toString: String = value
}

object Status {
    case object UNKNOWN extends Status { val value = "unknown" }
    case object RUNNING extends Status { val value = "running" }
    case object SUCCESS extends Status { val value = "success" }
    case object CLEANED extends Status { val value = "cleaned" }
    case object FAILED extends Status { val value = "failed" }
    case object ABORTED extends Status { val value = "killed" }
    case object SKIPPED extends Status { val value = "skipped" }

    def ofString(status:String) : Status = {
        status.toLowerCase(Locale.ROOT) match {
            case UNKNOWN.value => UNKNOWN
            case RUNNING.value => RUNNING
            case SUCCESS.value => SUCCESS
            case CLEANED.value => CLEANED
            case FAILED.value => FAILED
            case ABORTED.value => ABORTED
            case SKIPPED.value => SKIPPED
            case _ => throw new IllegalArgumentException(s"No status defined for '$status'")
        }
    }
}
