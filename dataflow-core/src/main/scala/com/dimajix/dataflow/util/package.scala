package com.dimajix.dataflow

package object util {
    def splitSettings(settings: Seq[String]) : Seq[(String,String)] = {
        settings.map(splitSetting)
    }
    def splitSetting(setting: String) : (String,String) = {
        val sep = setting.indexOf('=')
        (setting.take(sep), setting.drop(sep + 1).trim.replaceAll("^\"|\"$","").trim)
    }
}
