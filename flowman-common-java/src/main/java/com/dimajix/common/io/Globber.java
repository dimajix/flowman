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
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.io.Files;
import lombok.Getter;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Globber {
    private final Logger logger = LoggerFactory.getLogger(Globber.class);
    @Getter
    private final File rootFile;
    @Getter
    private final Path rootPath;
    private final FileSystem rootFs;

    public Globber(File root) throws IOException {
        this.rootFile = root.getAbsoluteFile().getCanonicalFile();
        this.rootPath = rootFile.toPath();
        this.rootFs = rootPath.getFileSystem();
    }

    public Stream<Path> glob() {
        val ignorePatterns = loadIgnoreFile(rootFile);
        val filter = new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(Path path) throws IOException {
                val relPath = rootPath.relativize(path);
                for (val p : ignorePatterns)
                    if (p.matches(relPath)) {
                        logger.debug("Ignoring " + path);
                        return false;
                    }
                return true;
            }
        };
        return streamDirectory(rootPath, filter)
            .filter(file -> !file.equals(rootPath));
    }

    private List<PathMatcher> loadIgnoreFile(File dir) {
        val ignoreFile = new File(dir, ".flowman-ignore");
        val ignorePatterns = new LinkedList<PathMatcher>();
        if (ignoreFile.isFile()) {
            try {
                val lines = Files.readLines(ignoreFile, StandardCharsets.UTF_8);
                lines.stream()
                    .map(String::trim)
                    .filter(l -> !l.startsWith("#"))
                    .filter(l -> !l.isEmpty())
                    .forEach(l -> ignorePatterns.add(rootFs.getPathMatcher("glob:" + l)));
            }
            catch(IOException ex) {
                logger.warn("Cannot read " + ignoreFile + ":\n  " + ex.getMessage());
            }
        }

        return ignorePatterns;
    }

    private Stream<Path> streamDirectory(Path dir, DirectoryStream.Filter<Path> filter) {
        try {
            val files = java.nio.file.Files.newDirectoryStream(dir, filter);
            return StreamSupport.stream(files.spliterator(), false)
                .flatMap(d -> java.nio.file.Files.isDirectory(d) ? Stream.concat(Stream.of(d), streamDirectory(d, filter)) : Stream.of(d));
        }
        catch (IOException ex) {
            logger.warn("Cannot read directory '" + dir + "'. Skipping.");
            return Stream.empty();
        }
    }
}
