package com.dimajix.flowman.templating

import com.dimajix.flowman.annotation.TemplateObject
import com.dimajix.flowman.spi.ClassAnnotationHandler


class TemplateObjectHandler extends ClassAnnotationHandler {
    override def annotation: Class[_] = classOf[TemplateObject]

    override def register(clazz: Class[_]): Unit = Velocity.addClass(clazz.getAnnotation(classOf[TemplateObject]).name(), clazz)
}
