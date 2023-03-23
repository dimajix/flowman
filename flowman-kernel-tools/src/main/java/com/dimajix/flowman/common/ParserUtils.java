/*
 * Copyright (C) 2020 The Flowman Authors
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

package com.dimajix.flowman.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.val;


public class ParserUtils {
    static public List<String> parseDelimitedList(String list)  {
        return Arrays.stream(list.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    static public Map<String,String> parseDelimitedKeyValues(String list) {
        return Arrays.stream(list.split(","))
            .map(String::trim)
            .filter(p -> p.contains("="))
            .map(p -> splitSetting(p))
            .filter(p -> !p.getKey().isEmpty())
            .collect(Collectors.toMap(Map.Entry<String,String>::getKey, Map.Entry<String,String>::getValue));
    }


    static public Map<String,String> splitSettings(String[] settings) {
        return splitSettings(Arrays.stream(settings));
    }

    static public Map<String,String> splitSettings(Iterable<String> settings) {
        val stream = StreamSupport.stream(settings.spliterator(),false);
        return splitSettings(stream);
    }
    static Map<String,String> splitSettings(Stream<String> settings) {
        return settings
            .map(ParserUtils::splitSetting)
            .collect(Collectors.toMap(Map.Entry<String,String>::getKey, Map.Entry<String,String>::getValue));
    }

    static public Map.Entry<String,String> splitSetting(String setting) {
        val sep = setting.indexOf('=');
        val key = setting.substring(0, sep).trim();
        val value = setting.substring(sep + 1).trim().replaceAll("^\"|\"$","");
        return Maps.immutableEntry(key, value);
    }
}
