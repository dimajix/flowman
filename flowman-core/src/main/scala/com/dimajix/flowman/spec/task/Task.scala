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

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spi.TypeRegistry


object Task extends TypeRegistry[Task] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "build", value = classOf[BuildTargetTask]),
    new JsonSubTypes.Type(name = "call", value = classOf[CallTask]),
    new JsonSubTypes.Type(name = "compareFiles", value = classOf[CompareFilesTask]),
    new JsonSubTypes.Type(name = "copyFile", value = classOf[CopyFileTask]),
    new JsonSubTypes.Type(name = "copyRelation", value = classOf[CopyRelationTask]),
    new JsonSubTypes.Type(name = "countMapping", value = classOf[CountMappingTask]),
    new JsonSubTypes.Type(name = "createRelation", value = classOf[CreateRelationTask]),
    new JsonSubTypes.Type(name = "deleteFile", value = classOf[DeleteFileTask]),
    new JsonSubTypes.Type(name = "destroyRelation", value = classOf[DestroyRelationTask]),
    new JsonSubTypes.Type(name = "describeRelation", value = classOf[DescribeRelationTask]),
    new JsonSubTypes.Type(name = "describeMapping", value = classOf[DescribeMappingTask]),
    new JsonSubTypes.Type(name = "getFile", value = classOf[GetFileTask]),
    new JsonSubTypes.Type(name = "loop", value = classOf[LoopTask]),
    new JsonSubTypes.Type(name = "mergeFiles", value = classOf[MergeFilesTask]),
    new JsonSubTypes.Type(name = "print", value = classOf[PrintTask]),
    new JsonSubTypes.Type(name = "putFile", value = classOf[PutFileTask]),
    new JsonSubTypes.Type(name = "showEnvironment", value = classOf[ShowEnvironmentTask]),
    new JsonSubTypes.Type(name = "showMapping", value = classOf[ShowMappingTask]),
    new JsonSubTypes.Type(name = "showRelation", value = classOf[ShowRelationTask]),
    new JsonSubTypes.Type(name = "shell", value = classOf[ShellTask]),
    new JsonSubTypes.Type(name = "sftpUpload", value = classOf[SftpUploadTask])
))
abstract class Task {
    /**
      * Abstract method which will perform the given task.
      *
      * @param executor
      */
    def execute(executor:Executor) : Boolean

    def description(implicit context:Context) : String
}
