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

package com.dimajix.common.io;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.stream.Collectors;

import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import com.dimajix.common.Resources;


public class GlobberTest {
    @Test
    public void testIgnoreFile() throws URISyntaxException, IOException {
        val url = Resources.getURL("com/dimajix/flowman/globber-test");
        val root = new File(url.toURI()).toPath();
        val globber = new Globber(new File(url.toURI()));
        val result = globber.glob().sorted().collect(Collectors.toList());

        val expected = Arrays.asList(
            root.resolve(".flowman-ignore"),
            root.resolve("subdir_2"),
            root.resolve("file-1.txt"),
            root.resolve("subdir_1"),
            root.resolve("subdir_1/subdir_2"),
            root.resolve("subdir_1/subdir_2/file-1.txt")
        ).stream().sorted().collect(Collectors.toList());
        assertIterableEquals(expected, result);
    }
}
