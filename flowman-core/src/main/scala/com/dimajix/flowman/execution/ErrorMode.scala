/*
 * Copyright (C) 2021 The Flowman Authors
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


sealed abstract class ErrorMode

object ErrorMode {
    case object FAIL_FAST extends ErrorMode
    case object FAIL_NEVER extends ErrorMode
    case object FAIL_AT_END extends ErrorMode

    def ofString(mode:String) : ErrorMode = {
        mode.toLowerCase(Locale.ROOT) match {
            case "failfast"|"fail_fast" => ErrorMode.FAIL_FAST
            case "failnever"|"fail_never" => ErrorMode.FAIL_NEVER
            case "failatend"|"fail_at_end" => ErrorMode.FAIL_AT_END
            case _ => throw new IllegalArgumentException(s"Unknown error mode: '$mode'. " +
                "Accepted error modes are 'failfast', 'failnever', 'failatend'.")
        }
    }
}
