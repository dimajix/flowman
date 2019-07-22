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

package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.AbstractInstance
import com.dimajix.flowman.spec.Instance
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.Spec
import com.dimajix.flowman.spi.TypeRegistry


object Task {
    object Properties {
        def apply(context:Context, kind:String="", description:Option[String]=None) : Properties = {
            require(context != null)
            Properties(
                context,
                context.namespace,
                context.project,
                kind,
                description
            )
        }
    }
    case class Properties(
         context: Context,
         namespace:Namespace,
         project:Project,
         kind:String,
         description:Option[String]
    ) extends Instance.Properties {
        override val name: String = ""
        override val labels: Map[String, String] = Map()
    }
}

abstract class Task extends AbstractInstance {
    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "task"

    /**
      * Abstract method which will perform the given task.
      *
      * @param executor
      */
    def execute(executor:Executor) : Boolean

    /**
      * Returns an optional description of this Task
      * @return
      */
    def description : Option[String]
}



object TaskSpec extends TypeRegistry[TaskSpec] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "build", value = classOf[BuildTargetTaskSpec]),
    new JsonSubTypes.Type(name = "call", value = classOf[CallTaskSpec]),
    new JsonSubTypes.Type(name = "compare", value = classOf[CompareTaskSpec]),
    new JsonSubTypes.Type(name = "compareFiles", value = classOf[CompareFilesTaskSpec]),
    new JsonSubTypes.Type(name = "copy", value = classOf[CopyTaskSpec]),
    new JsonSubTypes.Type(name = "copyFile", value = classOf[CopyFileTaskSpec]),
    new JsonSubTypes.Type(name = "copyRelation", value = classOf[CopyRelationTaskSpec]),
    new JsonSubTypes.Type(name = "countMapping", value = classOf[CountMappingTaskSpec]),
    new JsonSubTypes.Type(name = "createDatabase", value = classOf[CreateDatabaseTaskSpec]),
    new JsonSubTypes.Type(name = "createRelation", value = classOf[CreateRelationTaskSpec]),
    new JsonSubTypes.Type(name = "deleteFile", value = classOf[DeleteFileTaskSpec]),
    new JsonSubTypes.Type(name = "destroyRelation", value = classOf[DestroyRelationTaskSpec]),
    new JsonSubTypes.Type(name = "describeRelation", value = classOf[DescribeRelationTaskSpec]),
    new JsonSubTypes.Type(name = "describeMapping", value = classOf[DescribeMappingTaskSpec]),
    new JsonSubTypes.Type(name = "getFile", value = classOf[GetFileTaskSpec]),
    new JsonSubTypes.Type(name = "loop", value = classOf[LoopTaskSpec]),
    new JsonSubTypes.Type(name = "mergeFiles", value = classOf[MergeFilesTaskSpec]),
    new JsonSubTypes.Type(name = "print", value = classOf[PrintTaskSpec]),
    new JsonSubTypes.Type(name = "putFile", value = classOf[PutFileTaskSpec]),
    new JsonSubTypes.Type(name = "saveMapping", value = classOf[SaveMappingTaskSpec]),
    new JsonSubTypes.Type(name = "saveSchema", value = classOf[SaveSchemaTaskSpec]),
    new JsonSubTypes.Type(name = "show", value = classOf[ShowTaskSpec]),
    new JsonSubTypes.Type(name = "showEnvironment", value = classOf[ShowEnvironmentTaskSpec]),
    new JsonSubTypes.Type(name = "showMapping", value = classOf[ShowMappingTaskSpec]),
    new JsonSubTypes.Type(name = "showRelation", value = classOf[ShowRelationTaskSpec]),
    new JsonSubTypes.Type(name = "shell", value = classOf[ShellTaskSpec]),
    new JsonSubTypes.Type(name = "sftpUpload", value = classOf[SftpUploadTaskSpec])
))
abstract class TaskSpec extends Spec[Task] {
    @JsonProperty(value="kind", required=true) protected var kind:String = _
    @JsonProperty(value="description", required=false) protected var description:Option[String] = None

    override def instantiate(context:Context) : Task

    def instanceProperties(context: Context) : Task.Properties = {
        require(context != null)
        Task.Properties(
            context,
            context.namespace,
            context.project,
            kind,
            description.map(context.evaluate)
        )
    }
}
