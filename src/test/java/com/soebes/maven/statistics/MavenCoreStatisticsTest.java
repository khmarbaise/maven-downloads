package com.soebes.maven.statistics;

import org.apache.maven.artifact.versioning.ComparableVersion;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static com.soebes.maven.statistics.FileSelector.allFilesInDirectoryTree;
import static com.soebes.maven.statistics.Utility.unquote;
import static java.lang.System.out;
import static java.util.Map.Entry.comparingByKey;
import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summingLong;

/**
 * Grouping the Apache repository statistics.
 */
class MavenCoreStatisticsTest {

  static final Function<String, String[]> splitByComma = s -> s.split(",");
  static final Predicate<Path> onlyApacheMavenStatisticFiles = s -> s.getFileName().toString().startsWith("apache-maven-stats");
  static final Function<String[], Line> toLine = arr -> Line.of(unquote(arr[0]), unquote(arr[1]), unquote(arr[2]));
  static final ToLongFunction<Long> identity = __ -> __;
  static final String HEAD_LINE = "-".repeat(60);

  record MavenStats(ComparableVersion version, long numberOfDownloads, double relativeNumber) {
    static Function<Line, MavenStats> of = line -> new MavenStats(line.version(), line.numberOfDownloads(), line.relativeNumber());
  }

  record Line(ComparableVersion version, long numberOfDownloads, double relativeNumber) {
    static Line of(String version, String number, String relative) {
      return new Line(new ComparableVersion(version), Long.parseLong(number), Double.parseDouble(relative));
    }
  }

  static List<MavenStats> convert(Path csvFile) {
    try (var lines = Files.lines(csvFile)) {
      return lines.map(splitByComma)
          .map(toLine)
          .map(MavenStats.of)
          .toList();
    } catch (IOException e) {
      //Translate into RuntimeException.
      throw new RuntimeException(e);
    }
  }

  record YearMonth(int year, int month, List<MavenStats> lines) {
  }

  record YearMonthFile(int year, int month, Path fileName) {
    static YearMonthFile of(Path fileName) {
      String fileNameOnly = fileName.getFileName().toString();
      var month = Integer.parseInt(extractMonthOutOfFilename(fileNameOnly));
      var year = Integer.parseInt(extractYearOutOfFilename(fileNameOnly));
      return new YearMonthFile(year, month, fileName);
    }

  }

  private static String extractYearOutOfFilename(String fileNameOnly) {
    return fileNameOnly.substring(fileNameOnly.length() - 11, fileNameOnly.length() - 7);
  }

  private static String extractMonthOutOfFilename(String fileNameOnly) {
    return fileNameOnly.substring(fileNameOnly.length() - 6, fileNameOnly.length() - 4);
  }

  static List<YearMonth> readCSVStatistics(Path rootDirectory) throws IOException {
    var filesInDirectory = allFilesInDirectoryTree(rootDirectory);

    var listOfSelectedFiles = filesInDirectory.stream()
        .filter(onlyApacheMavenStatisticFiles)
        .toList();

    return listOfSelectedFiles
        .stream()
        .map(YearMonthFile::of)
        .map(ymf -> new YearMonth(ymf.year(), ymf.month(), convert(ymf.fileName())))
        .toList();
  }

  Comparator<YearMonth> byYearAndMonth = Comparator
      .comparingInt(YearMonth::year)
      .thenComparingInt(YearMonth::month);

  void mavenCoreStatistics(Path rootDirectory) throws IOException {
    out.println("Apache Maven Core Statistics");

    var mavenVersionStatistics = readCSVStatistics(rootDirectory);

    mavenVersionStatistics
        .stream()
        .sorted(byYearAndMonth)
        .forEachOrdered(s -> {
          var totalOverAllVersions = s.lines()
              .stream()
              .mapToLong(MavenStats::numberOfDownloads)
              .sum();
          out.printf("Year: %04d %02d %,10d %4d %n", s.year(), s.month(), totalOverAllVersions, s.lines().size());
        });


    var totalOfDownloadsOverallMavenVersions = mavenVersionStatistics
        .stream()
        .map(s -> s.lines().stream().mapToLong(MavenStats::numberOfDownloads).sum())
        .mapToLong(identity).sum();

    out.printf("totalOfDownloadsOverallMavenVersions: %,12d%n", totalOfDownloadsOverallMavenVersions);

    out.println(HEAD_LINE);
    var groupedByMavenVersion = mavenVersionStatistics.stream()
        .flatMap(s -> s.lines().stream())
        .collect(groupingBy(MavenStats::version, summingLong(MavenStats::numberOfDownloads)));

    groupedByMavenVersion
        .entrySet()
        .stream()
        .sorted(comparingByKey())
        .forEachOrdered(s -> out.printf("%-15s %,12d%n", s.getKey(), s.getValue()));

    var sum = groupedByMavenVersion
        .values()
        .stream()
        .mapToLong(identity)
        .sum();
    out.printf("%-15s %-12s%n", "=".repeat(13), "=".repeat(12));
    out.printf("%-15s %,12d%n", " ", sum);

    out.println("-".repeat(60));
    groupedByMavenVersion
        .entrySet()
        .stream()
        .sorted(comparingByValue(Comparator.reverseOrder()))
        .forEachOrdered(s -> {
          var percentage = s.getValue() / (double)sum * 100.0;
          out.printf("%-15s %,12d %6.2f%n", s.getKey(), s.getValue(), percentage);
        });

  }

  @Test
  void start() throws IOException {
    Locale.setDefault(Locale.US);
    var rootDirectory = Paths.get("src/test/resources");

    mavenCoreStatistics(rootDirectory);
  }
}
