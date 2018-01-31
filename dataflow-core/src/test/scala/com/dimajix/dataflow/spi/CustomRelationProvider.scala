package com.dimajix.dataflow.spi

import com.fasterxml.jackson.annotation.JsonTypeName

import com.dimajix.dataflow.spec.model.NullRelation


@JsonTypeName("customRelation")
class CustomRelation extends NullRelation {
}


class CustomRelationProvider extends RelationProvider {
    override def getName() : String = "customRelation"
    override def getImpl() : Class[_] = classOf[CustomRelation]
}
