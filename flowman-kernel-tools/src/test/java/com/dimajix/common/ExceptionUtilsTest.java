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

package com.dimajix.common;


import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExceptionUtilsTest {
    @Test
    public void testClassification() {
        assertTrue(ExceptionUtils.isFatal(new OutOfMemoryError()));
        assertTrue(ExceptionUtils.isFatal(new ThreadDeath()));
        assertTrue(ExceptionUtils.isFatal(new InterruptedException()));
        assertTrue(ExceptionUtils.isFatal(new LinkageError()));
        assertTrue(!ExceptionUtils.isFatal(new RuntimeException()));

        assertFalse(ExceptionUtils.isNonFatal(new OutOfMemoryError()));
        assertFalse(ExceptionUtils.isNonFatal(new ThreadDeath()));
        assertFalse(ExceptionUtils.isNonFatal(new InterruptedException()));
        assertFalse(ExceptionUtils.isNonFatal(new LinkageError()));
        assertFalse(!ExceptionUtils.isNonFatal(new RuntimeException()));
    }

    @Test
    public void testReasons() {
        val ex1 = new RuntimeException("This is a test");
        assertEquals("java.lang.RuntimeException: This is a test", ExceptionUtils.reasons(ex1));

        val ex2 = new RuntimeException("This is a test", new RuntimeException("second thing"));
        assertEquals("java.lang.RuntimeException: This is a test\n    caused by: java.lang.RuntimeException: second thing", ExceptionUtils.reasons(ex2));
    }
}
