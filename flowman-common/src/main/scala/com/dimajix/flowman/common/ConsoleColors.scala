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

package com.dimajix.flowman.common


object ConsoleColors {
    private var disabled = false
    def setColorEnabled(enabled:Boolean) : Unit = {
        disabled = !enabled
    }

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

    def white(str:String) : String = if (disabled) str else WHITE + str + NORMAL
    def boldWhite(str:String) : String = if (disabled) str else WHITE_BOLD + str + NORMAL
    def green(str:String) : String = if (disabled) str else GREEN + str + NORMAL
    def boldGreen(str:String) : String = if (disabled) str else GREEN_BOLD + str + NORMAL
    def red(str:String) : String = if (disabled) str else RED + str + NORMAL
    def boldRed(str:String) : String = if (disabled) str else RED_BOLD + str + NORMAL
    def yellow(str:String) : String = if (disabled) str else YELLOW + str + NORMAL
    def boldYellow(str:String) : String = if (disabled) str else YELLOW_BOLD + str + NORMAL
    def cyan(str:String) : String = if (disabled) str else CYAN + str + NORMAL
    def boldCyan(str:String) : String = if (disabled) str else CYAN_BOLD + str + NORMAL
}
