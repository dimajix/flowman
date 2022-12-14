/*
 * Copyright 2022 Kaya Kupferschmidt
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


sealed abstract class BuildPolicy extends Product with Serializable {

}


object BuildPolicy {
    case object ALWAYS extends BuildPolicy
    case object IF_EMPTY extends BuildPolicy
    case object IF_TAINTED extends BuildPolicy
    case object SMART extends BuildPolicy
    case object COMPAT extends BuildPolicy

    def ofString(policy:String) : BuildPolicy = {
        policy.toLowerCase(Locale.ROOT) match {
            case "always" => ALWAYS
            case "if_empty"|"ifempty" => IF_EMPTY
            case "if_tainted"|"iftainted" => IF_TAINTED
            case "smart" => SMART
            case "compat" => COMPAT
            case _ => throw new IllegalArgumentException(s"Unknown build policy: '$policy'. Accepted policies are 'always', 'smart', 'if_empty', 'if_tainted', 'compat'.")
        }
    }
}
