/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.studio.model

final case class Project(
    name:String,
    version:Option[String],
    description: Option[String],
    filename : Option[String],
    basedir : Option[String],
    environment: Map[String,String],
    config: Map[String,String],
    profiles: Seq[String],
    connections: Seq[String],
    jobs: Seq[String],
    targets: Seq[String]
)

final case class ProjectList(
    projects:Seq[Project]
)
