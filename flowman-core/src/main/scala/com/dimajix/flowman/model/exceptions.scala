/*
 * Copyright (C) 2022 The Flowman Authors
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


class ModelException(
    val message: String = "",
    val cause: Throwable = None.orNull
) extends Exception(message, cause) {
}


class DuplicateEntityException(
    val name: String,
    val category: Category,
    val firstOccurrence: String,
    val secondOccurrence: String,
    cause: Throwable = None.orNull
) extends ModelException(s"${category.upper} '$name' is defined twice:\n - $firstOccurrence\n - $secondOccurrence", cause)


class UnsupportedModuleFormatException(
    val format: String,
    cause : Throwable = None.orNull
) extends ModelException (s"Module format '$format' is not supported", cause)


class UnsupportedProjectFormatException(
    val format: String,
    cause : Throwable = None.orNull
) extends ModelException (s"Project format '$format' is not supported", cause)


class ModuleLoadException(
    source: String,
    cause: Throwable = None.orNull
) extends ModelException(s"Cannot load module from '$source'${if (cause != null) ": " + cause.getMessage else ""}", cause)


class ProjectLoadException(
    source: String,
    cause: Throwable = None.orNull
) extends ModelException(s"Cannot load project from '$source'${if (cause != null) ": " + cause.getMessage else ""}", cause)
