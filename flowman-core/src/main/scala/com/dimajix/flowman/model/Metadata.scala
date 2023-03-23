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

package com.dimajix.flowman.model

import com.dimajix.flowman.execution.Context


object Metadata {
    def apply(context: Context, name: String, category:Category, kind: String): Metadata =
        Metadata(
            context.namespace.map(_.name),
            context.project.map(_.name),
            name,
            context.project.flatMap(_.version),
            category.lower,
            kind,
            Map.empty
        )
}
final case class Metadata(
    namespace: Option[String] = None,
    project: Option[String] = None,
    name: String,
    version: Option[String] = None,
    category: String,
    kind: String,
    labels: Map[String,String] = Map.empty
) {
    require(name != null)
    require(category != null && category.nonEmpty)
    require(kind != null)

    /**
     * Merges this Metadata object with another one. Settings in the other object have higher priority, except for the
     * category and kind, which will not be changed at all.
     * @param other
     * @return
     */
    def merge(other:Metadata) : Metadata = {
        Metadata(
            other.namespace.orElse(namespace),
            other.project.orElse(project),
            other.name,
            other.version.orElse(version),
            category,
            kind,
            other.labels ++ labels
        )
    }

    /**
     * Returns the Metadata as a map, suitable to be used with a [[Context]]
     * @return
     */
    def asMap : Map[String,String] = {
        Map(
            "name" -> name,
            "category" -> category,
            "kind" -> kind
        ) ++
        namespace.map("namespace" -> _).toMap ++
        project.map("project" -> _).toMap ++
        version.map("version" -> _).toMap ++
        labels
    }
}
