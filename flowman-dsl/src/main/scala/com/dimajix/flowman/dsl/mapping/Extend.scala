package com.dimajix.flowman.dsl.mapping

import com.dimajix.flowman.dsl.MappingGen
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.spec.mapping.ExtendMapping

case class Extend(

) extends MappingGen {
    def apply(props:Mapping.Properties) : ExtendMapping = {
        val env = props.context.environment
        ???
    }
}
