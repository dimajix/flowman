/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.connection

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.annotation.JsonTypeResolver

import com.dimajix.common.TypeRegistry
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.annotation.ConnectionType
import com.dimajix.flowman.spec.template.CustomTypeResolverBuilder
import com.dimajix.flowman.spi.ClassAnnotationHandler


object ConnectionSpec extends TypeRegistry[ConnectionSpec] {
    final class NameResolver extends NamedSpec.NameResolver[ConnectionSpec]
}

/**
 * The ConnectionSpec class contains the raw specification values, which may require interpolation.
 */
@JsonTypeResolver(classOf[CustomTypeResolverBuilder])
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind", defaultImpl = classOf[JdbcConnectionSpec], visible = true)
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "jdbc", value = classOf[JdbcConnectionSpec]),
    new JsonSubTypes.Type(name = "ssh", value = classOf[SshConnectionSpec]),
    new JsonSubTypes.Type(name = "sftp", value = classOf[SshConnectionSpec])
))
abstract class ConnectionSpec extends NamedSpec[Connection] {
    /**
     * Creates an instance of this specification and performs the interpolation of all variables
     *
     * @param context
     * @return
     */
    override def instantiate(context: Context): Connection

    /**
     * Returns a set of common properties
     *
     * @param context
     * @return
     */
    override protected def instanceProperties(context: Context): Connection.Properties = {
        require(context != null)
        Connection.Properties(
            context,
            context.namespace,
            context.project,
            context.evaluate(name),
            kind,
            context.evaluate(labels)
        )
    }
}



class ConnectionSpecAnnotationHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[ConnectionType]

    override def register(clazz: Class[_]): Unit =
        ConnectionSpec.register(clazz.getAnnotation(classOf[ConnectionType]).kind(), clazz.asInstanceOf[Class[_ <: ConnectionSpec]])
}
