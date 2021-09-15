/*
 * Copyright 2020-2021 Kaya Kupferschmidt
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

import scala.collection.JavaConverters._

import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.templating.FileWrapper


object ProjectWrapper {
    def apply(project: Project) : ProjectWrapper = ProjectWrapper(Some(project))
}
final case class ProjectWrapper(project:Option[Project]) {
    def getBasedir() : FileWrapper = FileWrapper(project.flatMap(_.basedir).getOrElse(File.empty))
    def getFilename() : FileWrapper = FileWrapper(project.flatMap(_.filename).getOrElse(File.empty))
    def getName() : String = project.map(_.name).getOrElse("")
    def getVersion() : String = project.flatMap(_.version).getOrElse("")

    override def toString: String = getName()
}


object NamespaceWrapper {
    def apply(namespace: Namespace) : NamespaceWrapper = NamespaceWrapper(Some(namespace))
}
final case class NamespaceWrapper(namespace:Option[Namespace]) {
    def getName() : String = namespace.map(_.name).getOrElse("")
    override def toString: String = getName()
}


final case class JobWrapper(job:Job) {
    def getName() : String = job.name
    def getDescription() : String = job.description.getOrElse("")
    def getIdentifier() : String = job.identifier.toString
    def getProject() : ProjectWrapper = ProjectWrapper(job.project)
    def getNamespace() : NamespaceWrapper = NamespaceWrapper(job.namespace)

    override def toString: String = getName()
}


final case class TargetWrapper(target:Target) {
    def getName() : String = target.name
    def getIdentifier() : String = target.identifier.toString
    def getProject() : ProjectWrapper = ProjectWrapper(target.project)
    def getNamespace() : NamespaceWrapper = NamespaceWrapper(target.namespace)

    override def toString: String = getName()
}


final case class TestWrapper(test:Test) {
    def getName() : String = test.name
    def getDescription() : String = test.description.getOrElse("")
    def getIdentifier() : String = test.identifier.toString
    def getProject() : ProjectWrapper = ProjectWrapper(test.project)
    def getNamespace() : NamespaceWrapper = NamespaceWrapper(test.namespace)

    override def toString: String = getName()
}


final case class AssertionWrapper(assertion:Assertion) {
    def getName() : String = assertion.name
    def getDescription() : String = assertion.description.getOrElse("")
    def getProject() : ProjectWrapper = ProjectWrapper(assertion.project)
    def getNamespace() : NamespaceWrapper = NamespaceWrapper(assertion.namespace)

    override def toString: String = getName()
}


final case class LifecycleResultWrapper(result:LifecycleResult) {
    def getJob() : JobWrapper = JobWrapper(result.job)
    def getLifecycle() : java.util.List[String] = result.lifecycle.map(_.toString).asJava
    def getStatus() : String = result.status.toString
}


final case class JobResultWrapper(result:JobResult) {
    def getName() : String = result.job.name
    def getDescription() : String = result.job.description.getOrElse("")
    def getJob() : JobWrapper = JobWrapper(result.job)
    def getPhase() : String = result.phase.toString
    def getStatus() : String = result.status.toString
}


final case class TargetResultWrapper(result:TargetResult) {
    def getName() : String = result.target.name
    def getTarget() : TargetWrapper = TargetWrapper(result.target)
    def getPhase() : String = result.phase.toString
    def getStatus() : String = result.status.toString
}


final case class TestResultWrapper(result:TestResult) {
    def getName() : String = result.test.name
    def getDescription() : String = result.test.description.getOrElse("")
    def getTest() : TestWrapper = TestWrapper(result.test)
    def getStatus() : String = result.status.toString
}


final case class AssertionResultWrapper(result:AssertionResult) {
    def getName() : String = result.assertion.name
    def getAssertion() : AssertionWrapper = AssertionWrapper(result.assertion)
    def getStatus() : String = result.status.toString
}
