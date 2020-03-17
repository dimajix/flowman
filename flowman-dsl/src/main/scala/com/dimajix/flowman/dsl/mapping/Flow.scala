package com.dimajix.flowman.dsl.mapping

import com.dimajix.flowman.dsl.FieldMap
import com.dimajix.flowman.dsl.MappingGen
import com.dimajix.flowman.dsl.MappingList
import com.dimajix.flowman.dsl.WithWrapper
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.spec.mapping.UnitMapping


class Flow extends MappingGen with WithWrapper {
    val mappings : MappingList = new MappingList()
    val environment : FieldMap = new FieldMap()

    def apply(props:Mapping.Properties) : UnitMapping = {
        val env = props.context.environment
        UnitMapping(
            props,
            mappings,
            environment.toMap
        )
    }
}
