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
import com.dimajix.flowman.spi.ExtensionRegistry


object Task extends ExtensionRegistry[Task] {
}


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "kind")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "call", value = classOf[CallTask]),
    new JsonSubTypes.Type(name = "create-relation", value = classOf[CreateRelationTask]),
    new JsonSubTypes.Type(name = "destroy-relation", value = classOf[DestroyRelationTask]),
    new JsonSubTypes.Type(name = "describe-relation", value = classOf[DescribeRelationTask]),
    new JsonSubTypes.Type(name = "copy-relation", value = classOf[CopyRelationTask]),
    new JsonSubTypes.Type(name = "show-mapping", value = classOf[ShowMappingTask]),
    new JsonSubTypes.Type(name = "describe-mapping", value = classOf[DescribeMappingTask]),
    new JsonSubTypes.Type(name = "show-environment", value = classOf[ShowEnvironmentTask]),
    new JsonSubTypes.Type(name = "print", value = classOf[PrintTask]),
    new JsonSubTypes.Type(name = "loop", value = classOf[LoopTask]),
    new JsonSubTypes.Type(name = "output", value = classOf[OutputTask]),
    new JsonSubTypes.Type(name = "get-file", value = classOf[GetFileTask]),
    new JsonSubTypes.Type(name = "put-file", value = classOf[PutFileTask]),
    new JsonSubTypes.Type(name = "copy-file", value = classOf[CopyFileTask]),
    new JsonSubTypes.Type(name = "merge-files", value = classOf[MergeFilesTask]),
    new JsonSubTypes.Type(name = "delete-file", value = classOf[DeleteFileTask]),
    new JsonSubTypes.Type(name = "compare-files", value = classOf[CompareFilesTask]),
    new JsonSubTypes.Type(name = "sftp-upload", value = classOf[SftpUploadTask]),
    new JsonSubTypes.Type(name = "shell", value = classOf[ShellTask])
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
