package com.soebes.maven.statistics;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static com.soebes.maven.statistics.FileSelector.allFilesInDirectoryTree;
import static com.soebes.maven.statistics.Utility.unquote;
import static java.lang.System.out;
import static java.util.Map.Entry.comparingByKey;

class MavenPluginStatisticsTest {

  static final Function<String, String[]> splitByComa = s -> s.split(",");
  static final String HEAD_LINE = "-".repeat(60);
  static final List<String> DEFAULT_MAVEN_PLUGINS = List.of(
      "maven-clean-plugin",
      "maven-compiler-plugin",
      "maven-dependency-plugin",
      "maven-deploy-plugin",
      "maven-ear-plugin",
      "maven-ejb-plugin",
      "maven-enforcer-plugin",
      "maven-failsafe-plugin",
      "maven-help-plugin",
      "maven-install-plugin",
      "maven-jar-plugin",
      "maven-javadoc-plugin",
      "maven-resources-plugin",
      "maven-surefire-plugin",
      "maven-war-plugin"
  );

  record MavenPlugin(String plugin, long numberOfDownloads, double relativeNumber) {
    static MavenPlugin of(Line line) {
      return new MavenPlugin(line.plugin(), line.numberOfDownloads(), line.relativeNumber());
    }
  }

  record Line(String plugin, long numberOfDownloads, double relativeNumber) {
    static Line of(String plugin, String numberOfDownloads, String relative) {
      return new Line(plugin, Long.parseLong(numberOfDownloads), Double.parseDouble(relative));
    }
  }

  record YearMonthFile(int year, int month, Path fileName) {
    static YearMonthFile of(Path fileName) {
      String fileNameOnly = fileName.getFileName().toString();
      var month = Integer.parseInt(extractMonthFromFilename(fileNameOnly));
      var year = Integer.parseInt(extractYearFromFilename(fileNameOnly));
      return new YearMonthFile(year, month, fileName);
    }
  }

  private static String extractYearFromFilename(String fileNameOnly) {
    return fileNameOnly.substring(fileNameOnly.length() - 18, fileNameOnly.length() - 14);
  }

  private static String extractMonthFromFilename(String fileNameOnly) {
    return fileNameOnly.substring(fileNameOnly.length() - 13, fileNameOnly.length() - 11);
  }

  Comparator<YearMonth> byYearAndMonth = Comparator
      .comparingInt(YearMonth::year)
      .thenComparingInt(YearMonth::month);
  record YearMonth (int year, int month, List<MavenPlugin> lines) {
  }

  static List<MavenPlugin> convert(Path csvFile) {
    try (var lines = Files.lines(csvFile)) {
      return lines.map(splitByComa)
          .map(arr -> Line.of(unquote(arr[0]), unquote(arr[1]), unquote(arr[2])))
          .map(MavenPlugin::of)
          .toList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final Predicate<Path> byApacheMavenPlugins = s -> s.getFileName().toString().startsWith("org-apache-maven-plugins-");

  private final Comparator<MavenPlugin> byPlugin = Comparator.comparing(MavenPlugin::plugin);

  void mavenPluginStatistics(Path rootDirectory) throws IOException {
    out.println("Apache Maven Plugins Statistics");

    var paths = allFilesInDirectoryTree(rootDirectory);
    var listOfFiles = paths.stream()
        .filter(byApacheMavenPlugins)
        .toList();

    var mavenPluginStatistics = listOfFiles.stream()
        .map(YearMonthFile::of)
        .map(ymf -> new YearMonth(ymf.year(), ymf.month(), convert(ymf.fileName())))
        .toList();

    mavenPluginStatistics.stream()
        .sorted(byYearAndMonth)
        .forEach(s -> {
          var sumOfDownloadsPerMonth = s.lines().stream().mapToLong(MavenPlugin::numberOfDownloads).sum();
          out.printf("Year: %04d Month: %02d Number of plugins: %3d downloads: %,15d %n", s.year(), s.month(), s.lines().size(), sumOfDownloadsPerMonth);

          s.lines().stream()
              .sorted(byPlugin)
              .forEachOrdered(l -> out.printf(" %-36s %,10d%n", l.plugin(), l.numberOfDownloads()));

        });

    record PluginDownloadNumber(String plugin, long numberOfDownloads) {

    }
    var mavenAndStatistics = mavenPluginStatistics.stream()
        .flatMap(s -> s.lines().stream())
        .map(s -> new PluginDownloadNumber(s.plugin(), s.numberOfDownloads()))
        .collect(Collectors.groupingBy(PluginDownloadNumber::plugin, Collectors.summarizingLong(PluginDownloadNumber::numberOfDownloads)));

    var numberOfDownloadsTotal = mavenAndStatistics.values()
        .stream()
        .mapToLong(LongSummaryStatistics::getSum).sum();


    out.println("-".repeat(60));
    out.println(" Plugins ordered by plugin name.");
    out.println();

    mavenAndStatistics.entrySet()
        .stream()
        .sorted(comparingByKey())
        .forEachOrdered(plugin -> {
          var percentage = plugin.getValue().getSum() / (double)numberOfDownloadsTotal * 100;
          out.printf(" %-36s %,15d %6.2f%n", plugin.getKey(), plugin.getValue().getSum(), percentage);
        });

    out.println("-".repeat(60));
    out.printf(" %-36s %,15d%n", "numberOfDownloads of downloads in total:", numberOfDownloadsTotal);

    out.println(HEAD_LINE);
    out.println(" Plugins ordered by downloads.");
    out.println();

    mavenAndStatistics.entrySet()
        .stream()
        .sorted(Comparator.comparing(e -> e.getValue().getSum(), Comparator.reverseOrder()))
        .forEachOrdered(plugin -> {
          double percentage = plugin.getValue().getSum() / (double)numberOfDownloadsTotal * 100;
          out.printf(" %-36s %,15d %6.2f%n", plugin.getKey(), plugin.getValue().getSum(), percentage);
        });

    out.println("-".repeat(60));
    out.printf("Number of default plugins used: %3d%n", DEFAULT_MAVEN_PLUGINS.size());
    out.println("-".repeat(60));
    mavenAndStatistics.entrySet()
        .stream()
        .filter(plugin -> DEFAULT_MAVEN_PLUGINS.contains(plugin.getKey()))
        .forEach(plugin -> out.printf(" %-36s %,15d%n", plugin.getKey() , plugin.getValue().getSum()));

    final ToLongFunction<Map.Entry<String, LongSummaryStatistics>> theValue = s -> s.getValue().getSum();

    out.println("-".repeat(60));
    var numberOfDownloadsSelectedPlugins = mavenAndStatistics.entrySet()
        .stream()
        .filter(plugin -> DEFAULT_MAVEN_PLUGINS.contains(plugin.getKey()))
        .mapToLong(theValue).sum();

    out.printf(" %-36s %,15d%n", "numberOfDownloadsSelectedPlugins:", numberOfDownloadsSelectedPlugins);
  }

  @Test
  void start() throws IOException {
    Locale.setDefault(Locale.US);
    var rootDirectory = Paths.get("src/test/resources");

    mavenPluginStatistics(rootDirectory);
  }

}
