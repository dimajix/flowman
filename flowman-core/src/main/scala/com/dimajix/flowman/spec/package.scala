/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman

import com.dimajix.flowman.spec.connection.Connection
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.target.Target
import com.dimajix.flowman.spec.task.Job


package object spec {
    def splitSettings(settings: Seq[String]) : Seq[(String,String)] = {
        settings.map(splitSetting)
    }
    def splitSetting(setting: String) : (String,String) = {
        val sep = setting.indexOf('=')
        (setting.take(sep), setting.drop(sep + 1).trim.replaceAll("^\"|\"$","").trim)
    }

    type MappingIdentifier = Identifier[Mapping]
    type ConnectionIdentifier = Identifier[Connection]
    type RelationIdentifier = Identifier[Relation]
    type TargetIdentifier = Identifier[Target]
    type JobIdentifier = Identifier[Job]

    object MappingIdentifier extends IdentifierFactory[Mapping]
    object ConnectionIdentifier extends IdentifierFactory[Connection]
    object RelationIdentifier extends IdentifierFactory[Relation]
    object TargetIdentifier extends IdentifierFactory[Target]
    object JobIdentifier extends IdentifierFactory[Job]
}
