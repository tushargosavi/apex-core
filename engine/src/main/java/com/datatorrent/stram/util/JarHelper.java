/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.CodeSource;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.jar.JarArchiveEntry;
import org.apache.commons.io.IOUtils;

public class JarHelper
{
  private static final Logger logger = LoggerFactory.getLogger(JarHelper.class);

  private static final Map<URL, String> sourceToJar = new HashMap<>();

  public static String getJar(Class<?> jarClass)
  {
    String jar = null;
    final CodeSource codeSource = jarClass.getProtectionDomain().getCodeSource();
    if (codeSource != null) {
      URL location = codeSource.getLocation();
      jar = sourceToJar.get(location);
      if (jar == null) {
        // don't create jar file from folders multiple times
        if ("jar".equals(location.getProtocol())) {
          try {
            location = ((JarURLConnection)location.openConnection()).getJarFileURL();
          } catch (IOException e) {
            throw new AssertionError("Cannot resolve jar file for " + jarClass, e);
          }
        }
        if ("file".equals(location.getProtocol())) {
          jar = location.getFile();
          final File dir = new File(jar);
          if (dir.isDirectory()) {
            try {
              File temp = File.createTempFile("apex-", ".jar");
              new JarHelper(temp).createJar(dir);
              jar = temp.getAbsolutePath();
            } catch (IOException e) {
              throw new AssertionError("Cannot resolve jar file for " + jarClass + ". URL " + location, e);
            }
          }
        } else {
          throw new AssertionError("Cannot resolve jar file for " + jarClass + ". URL " + location);
        }
        sourceToJar.put(location, jar);
        logger.debug("added sourceLocation {} as {}", location, jar);
      }
      if (jar == null) {
        throw new AssertionError("Cannot resolve jar file for " + jarClass);
      }
    }
    return jar;
  }

  private final ArchiveOutputStream aos;

  private JarHelper(File file) throws IOException
  {
    try {
      aos = new ArchiveStreamFactory().createArchiveOutputStream(ArchiveStreamFactory.JAR, new FileOutputStream(file));
    } catch (ArchiveException e) {
      throw new IOException(e);
    }
  }

  private void createJar(File dir) throws IOException
  {
    File manifestFile = new File(dir, JarFile.MANIFEST_NAME);
    if (!manifestFile.exists()) {
      aos.putArchiveEntry(new JarArchiveEntry(JarFile.MANIFEST_NAME));
      new Manifest().write(aos);
      aos.closeArchiveEntry();
    } else {
      addEntry(manifestFile, JarFile.MANIFEST_NAME);
    }
    final java.nio.file.Path root = dir.toPath();
    Files.walkFileTree(root,
        new SimpleFileVisitor<Path>()
        {
          String relativePath;

          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
          {
            relativePath = root.relativize(dir).toString();
            if (!relativePath.isEmpty()) {
              if (!relativePath.endsWith("/")) {
                relativePath += "/";
              }
              addEntry(dir.toFile(), relativePath);
            }
            return super.preVisitDirectory(dir, attrs);
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
          {
            String name = relativePath + file.getFileName().toString();
            if (!JarFile.MANIFEST_NAME.equals(name)) {
              addEntry(file.toFile(), relativePath + file.getFileName().toString());
            }
            return super.visitFile(file, attrs);
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
          {
            relativePath = root.relativize(dir.getParent()).toString();
            if (!relativePath.isEmpty() && !relativePath.endsWith("/")) {
              relativePath += "/";
            }
            return super.postVisitDirectory(dir, exc);
          }
        }
    );
    aos.close();
  }

  private void addEntry(File file, String name) throws IOException
  {
    aos.putArchiveEntry(aos.createArchiveEntry(file, name));
    if (file.isFile()) {
      try (final FileInputStream input = new FileInputStream(file)) {
        IOUtils.copy(input, aos);
      }
    }
    aos.closeArchiveEntry();
  }
}
