/*
 * Copyright 2020-2022 Kaya Kupferschmidt
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


public class ConsoleColors {
    private static boolean disabled = false;
    public static void setColorEnabled(boolean enabled) {
        disabled = !enabled;
    }

    public static final String NORMAL = "\u001b[0m";
    public static final String BOLD = "\u001b[1m";
    public static final String CYAN = "\u001b[36m";
    public static final String CYAN_BOLD = CYAN + BOLD;
    public static final String YELLOW = "\u001b[33m";
    public static final String YELLOW_BOLD = YELLOW + BOLD;
    public static final String RED = "\u001b[31m";
    public static final String RED_BOLD = RED + BOLD;
    public static final String GREEN = "\u001b[32m";
    public static final String GREEN_BOLD = GREEN + BOLD;
    public static final String WHITE = "\u001b[37m";
    public static final String WHITE_BOLD = WHITE + BOLD;

    public static String white(String str)  { return disabled ? str : WHITE + str + NORMAL; }
    public static String boldWhite(String str)  { return disabled ? str : WHITE_BOLD + str + NORMAL; }
    public static String green(String str)  { return disabled ? str : GREEN + str + NORMAL; }
    public static String boldGreen(String str)  { return disabled ? str : GREEN_BOLD + str + NORMAL; }
    public static String red(String str)  { return disabled ? str : RED + str + NORMAL; }
    public static String boldRed(String str)  { return disabled ? str : RED_BOLD + str + NORMAL; }
    public static String yellow(String str)  { return disabled ? str : YELLOW + str + NORMAL; }
    public static String boldYellow(String str)  { return disabled ? str : YELLOW_BOLD + str + NORMAL; }
    public static String cyan(String str)  { return disabled ? str : CYAN + str + NORMAL; }
    public static String boldCyan(String str)  { return disabled ? str : CYAN_BOLD + str + NORMAL; }
}
