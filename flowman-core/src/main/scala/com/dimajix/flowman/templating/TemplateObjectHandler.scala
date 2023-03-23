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

package com.dimajix.flowman.templating

import com.dimajix.flowman.annotation.TemplateObject
import com.dimajix.flowman.spi.ClassAnnotationHandler


class TemplateObjectHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[TemplateObject]

    override def register(clazz: Class[_]): Unit = Velocity.addClass(clazz.getAnnotation(classOf[TemplateObject]).name(), clazz)
}
