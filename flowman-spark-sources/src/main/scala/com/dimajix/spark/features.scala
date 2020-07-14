package com.dimajix.spark

import scala.util.Try


object features {
    def hiveSupported: Boolean = Try {
        org.apache.hadoop.hive.shims.ShimLoader.getMajorVersion
        true
    }.getOrElse(false)
}
