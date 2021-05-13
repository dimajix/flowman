package com.dimajix.common.net

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.URL


object SocketUtils {
    def toURL(protocol:String, address:InetSocketAddress, allowAny:Boolean=false) : URL = {
        val localIpAddress = {
            if(allowAny)
                address.getAddress.getHostAddress
            else
                getLocalAddress(address)
        }
        val localPort = address.getPort
        new URL(protocol, localIpAddress, localPort, "")
    }
    def getLocalAddress(address:InetSocketAddress) : String = {
        if (address.getAddress.isAnyLocalAddress) {
            InetAddress.getLocalHost.getHostAddress
        }
        else {
            address.getAddress.getHostAddress
        }
    }
}
