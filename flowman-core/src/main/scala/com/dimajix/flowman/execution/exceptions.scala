/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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

import com.dimajix.flowman.model.ConnectionIdentifier
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TemplateIdentifier
import com.dimajix.flowman.model.TestIdentifier


class OperationException(
    val message: String = "",
    val cause: Throwable = None.orNull
) extends Exception(message, cause) {
}

class ExecutionException(
    val message: String = "",
    val cause: Throwable = None.orNull
) extends Exception(message, cause) {
}

class StateStoreException(
    val message: String = "",
    val cause: Throwable = None.orNull
) extends Exception(message, cause) {
}


class UnknownProjectException(val project:String)
    extends ExecutionException(s"Project '$project' not known, maybe you need to import it first.")
class ProjectNotFoundException(val project:String)
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
class NoSuchJobException(val job:JobIdentifier)
    extends ExecutionException(s"Job '$job' not found")
class NoSuchTestException(val test:TestIdentifier)
    extends ExecutionException(s"Test '$test' not found")
class NoSuchTemplateException(val template:TemplateIdentifier)
    extends ExecutionException(s"Template '$template' not found")

class DescribeMappingFailedException(val mapping:MappingIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Describing mapping '$mapping' failed", cause)
class DescribeRelationFailedException(val relation:RelationIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Describing relation '$relation' failed", cause)

class InstantiateConnectionFailedException(val connection:ConnectionIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Instantiating connection '$connection' failed", cause)
class InstantiateTargetFailedException(val target:TargetIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Instantiating target '$target' failed", cause)
class InstantiateJobFailedException(val job:JobIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Instantiating job '$job' failed", cause)
class InstantiateTestFailedException(val test:TestIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Instantiating test '$test' failed", cause)
class InstantiateTemplateFailedException(val template:TemplateIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Instantiating template '$template' failed", cause)
class InstantiateRelationFailedException(val relation:RelationIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Instantiating relation '$relation' failed", cause)
class InstantiateMappingFailedException(val mapping:MappingIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Instantiating mapping '$mapping' failed", cause)

class ValidationFailedException(val target:TargetIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Validation of target '$target' failed", cause)
class VerificationFailedException(val target:TargetIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Verification of target '$target' failed", cause)

class IncompatibleSchemaException(val relation:RelationIdentifier)
    extends ExecutionException(s"Incompatible schema in relation '$relation'")
class SchemaMismatchException(msg:String)
    extends ExecutionException(s"Mismatching schema: $msg")

class UnspecifiedSchemaException(val relation:RelationIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"No schema specified for '$relation' failed", cause)

class MigrationFailedException(val relation:RelationIdentifier, cause:Throwable = None.orNull)
    extends ExecutionException(s"Migration of '$relation' failed", cause)
