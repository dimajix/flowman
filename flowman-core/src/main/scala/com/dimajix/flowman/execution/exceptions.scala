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

package com.dimajix.flowman.execution

import com.dimajix.flowman.spec.BatchIdentifier
import com.dimajix.flowman.spec.ConnectionIdentifier
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.RelationIdentifier
import com.dimajix.flowman.spec.TargetIdentifier


class ExecutionException(
    val message: String = "",
    val cause: Throwable = None.orNull
) extends Exception(message, cause) {
}

class NoSuchProjectException(val project:String)
    extends ExecutionException(s"Project '$project' not found")
class NoSuchMappingException(val mapping:MappingIdentifier)
    extends ExecutionException(s"Mapping '$mapping' not found")
class NoSuchMappingOutputException(val output:MappingOutputIdentifier)
    extends ExecutionException(s"Mapping output '$output' not found") {
    def this(id:MappingIdentifier, output:String) = {
        this(MappingOutputIdentifier(id.name, output, id.project))
    }
}
class NoSuchRelationException(val relation:RelationIdentifier)
    extends ExecutionException(s"Relation '$relation' not found")
class NoSuchTargetException(val target:TargetIdentifier)
    extends ExecutionException(s"Target '$target' not found")
class NoSuchConnectionException(val connection:ConnectionIdentifier)
    extends ExecutionException(s"Connection '$connection' not found")
class NoSuchBundleException(val bundle:BatchIdentifier)
    extends ExecutionException(s"Bundle '$bundle' not found")

class VerificationFailedException(val target:TargetIdentifier)
    extends ExecutionException(s"Verification of target $target failed")
