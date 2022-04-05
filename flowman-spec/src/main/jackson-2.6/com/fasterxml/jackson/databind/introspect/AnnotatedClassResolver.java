package com.fasterxml.jackson.databind.introspect;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import scala.NotImplementedError;


public class AnnotatedClassResolver {
    public static AnnotatedClass resolve(MapperConfig<?> config, JavaType forType,
                                         ClassIntrospector.MixInResolver r) {
        throw new NotImplementedError();
    }
}
