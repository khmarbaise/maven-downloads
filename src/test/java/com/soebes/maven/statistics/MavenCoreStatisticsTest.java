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
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.soebes.maven.statistics.FileSelector.allFilesInDirectoryTree;
import static com.soebes.maven.statistics.Utility.unquote;
import static java.lang.System.out;

/**
 * Grouping the Apache repository statistics.
 */
class MavenCoreStatisticsTest {

  static final Function<String, String[]> splitByComma = s -> s.split(",");
  static final Predicate<Path> apacheMavenStatisticFiles = s -> s.getFileName().toString().startsWith("apache-maven-stats");
  static final Function<String[], Line> toLine = arr -> Line.of(unquote(arr[0]), unquote(arr[1]), unquote(arr[2]));

  record MavenStats(ComparableVersion version, long numberOfDownloads, double relativeNumber) {
    static MavenStats of(Line line) {
      return new MavenStats(line.version(), line.numberOfDownloads(), line.relativeNumber());
    }
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
          .map(MavenStats::of)
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
      int month = Integer.parseInt(extractMonthOutOfFilename(fileNameOnly));
      int year = Integer.parseInt(extractYearOutOfFilename(fileNameOnly));
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
        .filter(apacheMavenStatisticFiles)
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
        .stream().sorted(byYearAndMonth)
        .forEach(s -> {
          var totalOverAllVersions = s.lines()
              .stream()
              .mapToLong(MavenStats::numberOfDownloads).sum();
          out.printf("Year: %04d %02d %,10d %4d %n", s.year(), s.month(), totalOverAllVersions, s.lines().size());
        });


    var totalOfDownloadsOverallMavenVersions = mavenVersionStatistics
        .stream()
        .map(s -> s.lines().stream().mapToLong(MavenStats::numberOfDownloads).sum())
        .mapToLong(__ -> __).sum();

    out.printf("totalOfDownloadsOverallMavenVersions: %,12d%n", totalOfDownloadsOverallMavenVersions);

    out.println("-".repeat(60));
    var groupedByMavenVersion = mavenVersionStatistics.stream()
        .flatMap(s -> s.lines().stream())
        .collect(Collectors.groupingBy(MavenStats::version, Collectors.summingLong(MavenStats::numberOfDownloads)));

    groupedByMavenVersion
        .entrySet()
        .stream().sorted(Map.Entry.comparingByKey())
        .forEach(s -> out.printf("%-15s %,12d%n", s.getKey(), s.getValue()));


    var sum = groupedByMavenVersion.values().stream().mapToLong(l -> l).sum();
    out.printf("%-15s %-12s%n", "=".repeat(13), "=".repeat(12));
    out.printf("%-15s %,12d%n", " ", sum);

    out.println("-".repeat(60));
    groupedByMavenVersion
        .entrySet()
        .stream().sorted(Map.Entry.<ComparableVersion, Long>comparingByValue().reversed())
        .forEach(s -> {
          double percentage = s.getValue() / (double)sum * 100.0;
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
