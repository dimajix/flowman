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

package com.dimajix.common.text;


import java.util.Arrays;
import java.util.HashMap;

import com.google.common.collect.Maps;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;


public class ParserUtilsTest {
    @Test
    public void testParseDelimitedList() {
        assertIterableEquals(Arrays.asList("a","b","def"), ParserUtils.parseDelimitedList("a,b,,def,"));
        assertIterableEquals(Arrays.asList("a","b","def"), ParserUtils.parseDelimitedList("a , b  ,  ,def,"));
    }

    @Test
    public void testParseDelimitedKeyValues() {
        val result = ParserUtils.parseDelimitedKeyValues("x=12,b,c=,,yz=\"This is some test\",");
        val expected = new HashMap<String,String>();
        expected.put("x", "12");
        expected.put("c", "");
        expected.put("yz", "This is some test");
        assertIterableEquals(expected.entrySet(), result.entrySet());
    }

    @Test
    public void testSplitSettings1() {
        val result = ParserUtils.splitSettings(new String[]{"x=12", "b", "c=", "yz=\"This is some test\""});
        val expected = new HashMap<String,String>();
        expected.put("x", "12");
        expected.put("b", "");
        expected.put("c", "");
        expected.put("yz", "This is some test");
        assertIterableEquals(expected.entrySet(), result.entrySet());
    }

    @Test
    public void testSplitSettings2() {
        val result = ParserUtils.splitSettings(Arrays.asList("x=12","b","c=","yz=\"This is some test\""));
        val expected = new HashMap<String,String>();
        expected.put("x", "12");
        expected.put("b", "");
        expected.put("c", "");
        expected.put("yz", "This is some test");
        assertIterableEquals(expected.entrySet(), result.entrySet());
    }

    @Test
    public void testSplitSetting() {
        assertEquals(Maps.immutableEntry("a", ""), ParserUtils.splitSetting("a="));
        assertEquals(Maps.immutableEntry("a", "12"), ParserUtils.splitSetting("a=12"));
        assertEquals(Maps.immutableEntry("a", "12=23"), ParserUtils.splitSetting("a=12=23"));
        assertEquals(Maps.immutableEntry("yz", "This is a test"), ParserUtils.splitSetting("yz=\"This is a test\""));
    }
}
