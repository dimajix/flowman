/*
 * Copyright (C) 2019 The Flowman Authors
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

package com.dimajix.flowman.server.model


final case class ProjectDocumentation (
    name: String,
    version: Option[String] = None,
    description: Option[String] = None,
    relations: Seq[RelationDocumentation] = Seq.empty
)

final case class RelationDocumentation (
    parent: Option[String],
    kind: String,
    identifier: Identifier,
    description: Option[String],
    schema: Option[SchemaDocumentation],
    inputs: Seq[String],
    provides: Seq[Resource],
    requires: Seq[Resource],
    sources: Seq[Resource]
)

final case class SchemaDocumentation (
    parent: Option[String],
    description: Option[String],
    columns: Seq[ColumnDocumentation]
)

final case class ColumnDocumentation (
    parent: Option[String],
    name: String,
    fqName: String,
    typeName: String,
    sqlType: String,
    nullable: Boolean,
    description: Option[String],
    children: Seq[ColumnDocumentation],
    inputs: Seq[String],
    index: Int
)
