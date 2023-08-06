package com.soebes.maven.statistics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;

interface FileSelector {

  Predicate<Path> IS_REGULAR_FILE = Files::isRegularFile;
  Predicate<Path> IS_READABLE = Files::isReadable;
  Predicate<Path> IS_VALID_FILE = IS_REGULAR_FILE.and(IS_READABLE);

  static List<Path> allFilesInDirectoryTree(Path start) throws IOException {
    try (var pathStream = Files.walk(start)) {
      return pathStream.filter(IS_VALID_FILE).toList();
    }
  }


}
