/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.common.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import lombok.val;


public final class SocketUtils {
    private SocketUtils() {
    }

    public static URL toURL(String protocol, InetSocketAddress address, boolean allowAny) throws UnknownHostException, MalformedURLException {
        val localIpAddress = allowAny ? address.getAddress().getHostAddress() : getLocalAddress(address);
        val localPort = address.getPort();
        return new URL(protocol, localIpAddress, localPort, "");
    }

    public static String getLocalAddress(InetSocketAddress address) throws UnknownHostException {
        if (address.getAddress().isAnyLocalAddress()) {
            return InetAddress.getLocalHost().getHostAddress();
        }
        else {
            return address.getAddress().getHostAddress();
        }
    }
}
