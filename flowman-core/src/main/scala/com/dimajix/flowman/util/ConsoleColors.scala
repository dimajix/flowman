/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.util

object ConsoleColors {
    val NORMAL = Console.RESET
    val CYAN = Console.CYAN
    val CYAN_BOLD = Console.CYAN + Console.BOLD
    val YELLOW = Console.YELLOW
    val YELLOW_BOLD = Console.YELLOW + Console.BOLD
    val RED = Console.RED
    val RED_BOLD = Console.RED + Console.BOLD
    val GREEN = Console.GREEN
    val GREEN_BOLD = Console.GREEN + Console.BOLD
    val WHITE = Console.WHITE
    val WHITE_BOLD = Console.WHITE + Console.BOLD

    def white(str:String) : String = WHITE + str + NORMAL
    def boldWhite(str:String) : String = WHITE_BOLD + str + NORMAL
    def green(str:String) : String = GREEN + str + NORMAL
    def boldGreen(str:String) : String = GREEN_BOLD + str + NORMAL
    def red(str:String) : String = RED + str + NORMAL
    def boldRed(str:String) : String = RED_BOLD + str + NORMAL
    def yellow(str:String) : String = YELLOW + str + NORMAL
    def boldYellow(str:String) : String = YELLOW_BOLD + str + NORMAL
    def cyan(str:String) : String = CYAN + str + NORMAL
    def boldCyan(str:String) : String = CYAN_BOLD + str + NORMAL
}
