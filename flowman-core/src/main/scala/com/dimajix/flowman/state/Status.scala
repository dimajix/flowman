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

package com.dimajix.flowman.state


sealed abstract class Status { val value:String }
object Status {
    case object RUNNING extends Status { val value = "running" }
    case object SUCCESS extends Status { val value = "success" }
    case object FAILED extends Status { val value = "failed" }
    case object ABORTED extends Status { val value = "killed" }
    case object SKIPPED extends Status { val value = "skipped" }
}
