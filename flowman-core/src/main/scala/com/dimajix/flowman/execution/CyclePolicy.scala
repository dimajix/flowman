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



sealed abstract class CyclePolicy extends Product with Serializable {
}

object CyclePolicy {
    case object ALWAYS extends CyclePolicy
    case object NEVER extends CyclePolicy
    case object FIRST extends CyclePolicy
    case object LAST extends CyclePolicy

    def ofString(name: String): CyclePolicy = {
        name.toLowerCase(Locale.ROOT) match {
            case "always" => ALWAYS
            case "never" => NEVER
            case "first" => FIRST
            case "last" => LAST
            case _ => throw new IllegalArgumentException(s"No phase cycle policy defined for '$name'. Possible values are 'always', 'never', 'first' and 'last'")
        }
    }
}
