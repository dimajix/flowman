/*
 * Copyright 2020 Kaya Kupferschmidt
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

import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.templating.FileWrapper


object ProjectWrapper {
    def apply(project: Project) : ProjectWrapper = ProjectWrapper(Some(project))
}
case class ProjectWrapper(project:Option[Project]) {
    def getBasedir() : FileWrapper = FileWrapper(project.flatMap(_.basedir).getOrElse(File.empty))
    def getFilename() : FileWrapper = FileWrapper(project.flatMap(_.filename).getOrElse(File.empty))
    def getName() : String = project.map(_.name).getOrElse("")
    def getVersion() : String = project.flatMap(_.version).getOrElse("")

    override def toString: String = getName()
}


object NamespaceWrapper {
    def apply(namespace: Namespace) : NamespaceWrapper = NamespaceWrapper(Some(namespace))
}
case class NamespaceWrapper(namespace:Option[Namespace]) {
    def getName() : String = namespace.map(_.name).getOrElse("")
    override def toString: String = getName()
}


case class JobWrapper(job:Job) {
    def getName() : String = job.name
    def getProject() : ProjectWrapper = ProjectWrapper(job.project)
    def getNamespace() : NamespaceWrapper = NamespaceWrapper(job.namespace)

    override def toString: String = getName()
}


case class TestWrapper(test:Test) {
    def getName() : String = test.name
    def getProject() : ProjectWrapper = ProjectWrapper(test.project)
    def getNamespace() : NamespaceWrapper = NamespaceWrapper(test.namespace)

    override def toString: String = getName()
}
