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

package com.dimajix.flowman.spec.target

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.annotation.TargetType
import com.dimajix.flowman.spi.ClassAnnotationHandler


object TargetSpec extends TypeRegistry[TargetSpec] {
    class NameResolver extends NamedSpec.NameResolver[TargetSpec]
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "blackhole", value = classOf[BlackholeTargetSpec]),
    new JsonSubTypes.Type(name = "compare", value = classOf[CompareTargetSpec]),
    new JsonSubTypes.Type(name = "console", value = classOf[ConsoleTargetSpec]),
    new JsonSubTypes.Type(name = "copy", value = classOf[CopyTargetSpec]),
    new JsonSubTypes.Type(name = "copyFile", value = classOf[CopyFileTargetSpec]),
    new JsonSubTypes.Type(name = "count", value = classOf[CountTargetSpec]),
    new JsonSubTypes.Type(name = "deleteFile", value = classOf[DeleteFileTargetSpec]),
    new JsonSubTypes.Type(name = "file", value = classOf[FileTargetSpec]),
    new JsonSubTypes.Type(name = "getFile", value = classOf[GetFileTargetSpec]),
    new JsonSubTypes.Type(name = "hiveDatabase", value = classOf[HiveDatabaseTargetSpec]),
    new JsonSubTypes.Type(name = "local", value = classOf[LocalTargetSpec]),
    new JsonSubTypes.Type(name = "merge", value = classOf[MergeTargetSpec]),
    new JsonSubTypes.Type(name = "mergeFiles", value = classOf[MergeFilesTargetSpec]),
    new JsonSubTypes.Type(name = "null", value = classOf[NullTargetSpec]),
    new JsonSubTypes.Type(name = "putFile", value = classOf[PutFileTargetSpec]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationTargetSpec]),
    new JsonSubTypes.Type(name = "schema", value = classOf[SchemaTargetSpec]),
    new JsonSubTypes.Type(name = "sftpUpload", value = classOf[SftpUploadTargetSpec]),
    new JsonSubTypes.Type(name = "stream", value = classOf[StreamTargetSpec]),
    new JsonSubTypes.Type(name = "template", value = classOf[TemplateTargetSpec]),
    new JsonSubTypes.Type(name = "validate", value = classOf[ValidateTargetSpec]),
    new JsonSubTypes.Type(name = "verify", value = classOf[VerifyTargetSpec])
))
abstract class TargetSpec extends NamedSpec[Target] {
    @JsonProperty(value = "before", required=false) private var before:Seq[String] = Seq()
    @JsonProperty(value = "after", required=false) private var after:Seq[String] = Seq()

    override def instantiate(context: Context): Target

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    override protected def instanceProperties(context:Context) : Target.Properties = {
        require(context != null)
        Target.Properties(
            context,
            context.namespace,
            context.project,
            name,
            kind,
            context.evaluate(labels),
            before.map(context.evaluate).map(TargetIdentifier.parse),
            after.map(context.evaluate).map(TargetIdentifier.parse)
        )
    }
}



class TargetSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[TargetType]

    override def register(clazz: Class[_]): Unit =
        TargetSpec.register(clazz.getAnnotation(classOf[TargetType]).kind(), clazz.asInstanceOf[Class[_ <: TargetSpec]])
}
