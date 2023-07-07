package com.dimajix.flowman.jersey

import java.lang.annotation.Annotation
import java.lang.reflect.Type

import javax.inject.Inject
import javax.inject.Singleton
import javax.ws.rs.ext.ParamConverter
import javax.ws.rs.ext.ParamConverterProvider
import javax.ws.rs.ext.Provider
import org.glassfish.hk2.api.ServiceLocator
import org.glassfish.jersey.internal.inject.Providers
import org.glassfish.jersey.internal.util.ReflectionHelper


@Provider
@Singleton
class OptionParamConverterProvider @Inject() (
    serviceLocator: ServiceLocator
) extends ParamConverterProvider {
    /**
     * {@inheritDoc }
     */
    override def getConverter[T](rawType: Class[T], genericType: Type, annotations: Array[Annotation]): ParamConverter[T] = {
        if (classOf[Option[_]] == rawType) {
            val ctps = ReflectionHelper.getTypeArgumentAndClass(genericType)
            val ctp = if (ctps.size == 1) ctps.get(0) else null
            if (ctp == null || (ctp.rawClass eq classOf[String])) {
                return new ParamConverter[T]() {
                    override def fromString(value: String): T = rawType.cast(Option(value))
                    override def toString(value: T): String = value.toString
                }
            }

            if (ctp eq classOf[Int]) {
                return new ParamConverter[T]() {
                    override def fromString(value: String): T = rawType.cast(Option(value).map(_.toInt))
                    override def toString(value: T): String = value.toString
                }
            }

            val converterProviders = Providers.getProviders(serviceLocator, classOf[ParamConverterProvider])
            import scala.collection.JavaConversions._
            for (provider <- converterProviders) {
                val converter = provider.getConverter(ctp.rawClass, ctp.`type`, annotations)
                if (converter != null) {
                    return new ParamConverter[T]() {
                        override def fromString(value: String): T = rawType.cast(Option(value).map(converter.fromString))
                        override def toString(value: T): String = value.toString
                    }
                }
            }
        }
        null
    }
}
