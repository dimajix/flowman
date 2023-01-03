/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalTempDir


class FileGlobTest extends AnyFlatSpec with Matchers with LocalTempDir {
    private val localFs = org.apache.hadoop.fs.FileSystem.getLocal(new Configuration())

    "FileGlob" should "provide a nice toString representation" in {
        val dir = HadoopFile(localFs, new fs.Path(tempDir.toURI.toString))

        FileGlob(dir, None).toString should be (dir.toString)
        FileGlob(dir, Some("*")).toString should be ((dir / "*").toString)
        FileGlob(dir, Some("abc")).toString should be ((dir / "abc").toString)
        FileGlob(dir, Some("/abc")).toString should be ((dir / "/abc").toString)
        FileGlob(dir, Some("abc/")).toString should be ((dir / "abc/").toString)
        FileGlob(dir, Some("/abc/")).toString should be ((dir / "/abc/").toString)

        FileGlob(dir, Some("abc")).toString should be((dir / "abc").toString)
        //FileGlob(dir, Some("/abc")).toString should be((dir / "abc").toString)
        FileGlob(dir, Some("abc/")).toString should be((dir / "abc").toString)
        //FileGlob(dir, Some("/abc/")).toString should be((dir / "abc").toString)
    }

    it should "provide a meaningful path" in {
        val dir = HadoopFile(localFs, new fs.Path(tempDir.toURI.toString))

        FileGlob(dir, None).path should be(dir.path)
        FileGlob(dir, Some("*")).path should be((dir / "*").path)
        FileGlob(dir, Some("abc")).path should be((dir / "abc").path)
        //FileGlob(dir, Some("/abc")).path should be((dir / "/abc").path)
        FileGlob(dir, Some("abc/")).path should be((dir / "abc/").path)
        //FileGlob(dir, Some("/abc/")).path should be((dir / "/abc/").path)

        FileGlob(dir, Some("abc")).path should be((dir / "abc").path)
        //FileGlob(dir, Some("/abc")).path should be((dir / "abc").path)
        FileGlob(dir, Some("abc/")).path should be((dir / "abc").path)
        //FileGlob(dir, Some("/abc/")).path should be((dir / "abc").path)
    }

    it should "provide a meaningful file" in {
        val dir = HadoopFile(localFs, new fs.Path(tempDir.toURI.toString))

        FileGlob(dir, None).file should be(dir)
        FileGlob(dir, Some("*")).file should be((dir / "*"))
        FileGlob(dir, Some("abc")).file should be((dir / "abc"))
        //FileGlob(dir, Some("/abc")).file should be((dir / "/abc"))
        FileGlob(dir, Some("abc/")).file should be((dir / "abc/"))
        //FileGlob(dir, Some("/abc/")).file should be((dir / "/abc/"))

        FileGlob(dir, Some("abc")).file should be((dir / "abc"))
        //FileGlob(dir, Some("/abc")).file should be((dir / "abc"))
        FileGlob(dir, Some("abc/")).file should be((dir / "abc"))
        //FileGlob(dir, Some("/abc/")).file should be((dir / "abc"))
    }
}
