/*
 * Copyright (C) 2018 The Flowman Authors
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
import com.fasterxml.jackson.annotation.JsonProperty.Access
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonTypeResolver

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.annotation.TargetType
import com.dimajix.flowman.spec.documentation.TargetDocSpec
import com.dimajix.flowman.spec.template.CustomTypeResolverBuilder
import com.dimajix.flowman.spec.template.TargetTemplateInstanceSpec
import com.dimajix.flowman.spi.ClassAnnotationHandler


object TargetSpec extends TypeRegistry[TargetSpec] {
    final class NameResolver extends NamedSpec.NameResolver[TargetSpec]
}


@JsonTypeResolver(classOf[CustomTypeResolverBuilder])
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "blackhole", value = classOf[BlackholeTargetSpec]),
    new JsonSubTypes.Type(name = "compare", value = classOf[CompareTargetSpec]),
    new JsonSubTypes.Type(name = "console", value = classOf[ConsoleTargetSpec]),
    new JsonSubTypes.Type(name = "copy", value = classOf[CopyTargetSpec]),
    new JsonSubTypes.Type(name = "copyFile", value = classOf[CopyFileTargetSpec]),
    new JsonSubTypes.Type(name = "count", value = classOf[CountTargetSpec]),
    new JsonSubTypes.Type(name = "deleteFile", value = classOf[DeleteFileTargetSpec]),
    new JsonSubTypes.Type(name = "document", value = classOf[DocumentTargetSpec]),
    new JsonSubTypes.Type(name = "documentation", value = classOf[DocumentTargetSpec]),
    new JsonSubTypes.Type(name = "drop", value = classOf[DropTargetSpec]),
    new JsonSubTypes.Type(name = "empty", value = classOf[EmptyTargetSpec]),
    new JsonSubTypes.Type(name = "file", value = classOf[FileTargetSpec]),
    new JsonSubTypes.Type(name = "getFile", value = classOf[GetFileTargetSpec]),
    new JsonSubTypes.Type(name = "hiveDatabase", value = classOf[HiveDatabaseTargetSpec]),
    new JsonSubTypes.Type(name = "jdbcCommand", value = classOf[JdbcCommandTargetSpec]),
    new JsonSubTypes.Type(name = "local", value = classOf[LocalTargetSpec]),
    new JsonSubTypes.Type(name = "measure", value = classOf[MeasureTargetSpec]),
    new JsonSubTypes.Type(name = "merge", value = classOf[MergeTargetSpec]),
    new JsonSubTypes.Type(name = "mergeFiles", value = classOf[MergeFilesTargetSpec]),
    new JsonSubTypes.Type(name = "putFile", value = classOf[PutFileTargetSpec]),
    new JsonSubTypes.Type(name = "relation", value = classOf[RelationTargetSpec]),
    new JsonSubTypes.Type(name = "schema", value = classOf[SchemaTargetSpec]),
    new JsonSubTypes.Type(name = "stream", value = classOf[StreamTargetSpec]),
    new JsonSubTypes.Type(name = "template", value = classOf[TemplateTargetSpec]),
    new JsonSubTypes.Type(name = "truncate", value = classOf[TruncateTargetSpec]),
    new JsonSubTypes.Type(name = "validate", value = classOf[ValidateTargetSpec]),
    new JsonSubTypes.Type(name = "verify", value = classOf[VerifyTargetSpec]),
    new JsonSubTypes.Type(name = "template/*", value = classOf[TargetTemplateInstanceSpec])
))
abstract class TargetSpec extends NamedSpec[Target] {
    @JsonProperty(value="kind", access=Access.WRITE_ONLY, required=true) protected var kind: String = _
    @JsonProperty(value="before", required=false) protected[spec] var before:Seq[String] = Seq.empty
    @JsonProperty(value="after", required=false) protected[spec] var after:Seq[String] = Seq.empty
    @JsonProperty(value="description", required = false) private var description: Option[String] = None
    @JsonProperty(value="documentation", required=false) private var documentation: Option[TargetDocSpec] = None

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): Target

    /**
      * Returns a set of common properties
      * @param context
      * @return
      */
    override protected def instanceProperties(context:Context, properties:Option[Target.Properties]) : Target.Properties = {
        require(context != null)
        val name = context.evaluate(this.name)
        val props = Target.Properties(
            context,
            metadata.map(_.instantiate(context, name, Category.TARGET, kind)).getOrElse(Metadata(context, name, Category.TARGET, kind)),
            before.map(context.evaluate).map(TargetIdentifier.parse),
            after.map(context.evaluate).map(TargetIdentifier.parse),
            context.evaluate(description),
            documentation.map(_.instantiate(context))
        )
        properties.map(p => props.merge(p)).getOrElse(props)
    }
}



class TargetSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[TargetType]

    override def register(clazz: Class[_]): Unit =
        TargetSpec.register(clazz.getAnnotation(classOf[TargetType]).kind(), clazz.asInstanceOf[Class[_ <: TargetSpec]])
}
