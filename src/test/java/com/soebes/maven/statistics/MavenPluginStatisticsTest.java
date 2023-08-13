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
import java.util.stream.Collectors;

import static com.soebes.maven.statistics.FileSelector.allFilesInDirectoryTree;
import static com.soebes.maven.statistics.Utility.unquote;
import static java.lang.System.out;

class MavenPluginStatisticsTest {

  List<String> DEFAULT_MAVEN_PLUGINS = List.of(
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
      int month = Integer.parseInt(fileNameOnly.substring(fileNameOnly.length() - 13, fileNameOnly.length() - 11));
      int year = Integer.parseInt(fileNameOnly.substring(fileNameOnly.length() - 18, fileNameOnly.length() - 14));
      return new YearMonthFile(year, month, fileName);
    }
  }

  Comparator<YearMonth> YEAR_MONTH_COMPARATOR = Comparator
      .comparingInt(YearMonth::year)
      .thenComparingInt(YearMonth::month);
  record YearMonth (int year, int month, List<MavenPlugin> lines) {
  }

  static List<MavenPlugin> convert(Path csvFile) {
    try (var lines = Files.lines(csvFile)) {
      return lines.map(s -> s.split(","))
          .map(arr -> Line.of(unquote(arr[0]), unquote(arr[1]), unquote(arr[2])))
          .map(MavenPlugin::of)
          .toList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  void mavenPluginStatistics(Path rootDirectory) throws IOException {
    out.println("Apache Maven Plugins Statistics");

    var paths = allFilesInDirectoryTree(rootDirectory);
    var listOfFiles = paths.stream()
        .filter(s -> s.getFileName().toString().startsWith("org-apache-maven-plugins"))
        .toList();

    var mavenPluginStatistics = listOfFiles.stream()
        .map(YearMonthFile::of)
        .map(ymf -> new YearMonth(ymf.year(), ymf.month(), convert(ymf.fileName())))
        .toList();

    mavenPluginStatistics.stream()
        .sorted(YEAR_MONTH_COMPARATOR)
        .forEach(s -> {
          var sumOfDownloadsPerMonth = s.lines().stream().mapToLong(MavenPlugin::numberOfDownloads).sum();
          out.printf("Year: %04d Month: %02d Number of plugins: %3d downloads: %,15d %n", s.year(), s.month(), s.lines().size(), sumOfDownloadsPerMonth);

          s.lines().stream()
              .sorted(Comparator.comparing(MavenPlugin::plugin))
              .forEachOrdered(l -> out.printf(" %-36s %,10d%n", l.plugin(), l.numberOfDownloads()));

        });

    record PluginDownloadNumber(String plugin, long numberOfDownloads) {

    }
    var collect = mavenPluginStatistics.stream()
        .flatMap(s -> s.lines().stream())
        .map(s -> new PluginDownloadNumber(s.plugin(), s.numberOfDownloads()))
        .collect(Collectors.groupingBy(PluginDownloadNumber::plugin, Collectors.summarizingLong(PluginDownloadNumber::numberOfDownloads)));

    var numberOfDownloadsTotal = collect.values()
        .stream()
        .mapToLong(LongSummaryStatistics::getSum).sum();


    out.println("-".repeat(60));
    out.println(" Plugins ordered by plugin name.");
    out.println();

    collect.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(plugin -> {
          double percentage = plugin.getValue().getSum() / (double)numberOfDownloadsTotal * 100;
          out.printf(" %-36s %,15d %6.2f%n", plugin.getKey(), plugin.getValue().getSum(), percentage);
        });

    out.println("-".repeat(60));
    out.printf(" %-36s %,15d%n", "numberOfDownloads of downloads in total:", numberOfDownloadsTotal);

    out.println("-".repeat(60));
    out.println(" Plugins ordered by downloads.");
    out.println();

    collect.entrySet()
        .stream()
        .sorted(Comparator.<Map.Entry<String, LongSummaryStatistics>>comparingLong(e -> e.getValue().getSum()).reversed())
        .forEach(plugin -> {
          double percentage = plugin.getValue().getSum() / (double)numberOfDownloadsTotal * 100;
          out.printf(" %-36s %,15d %6.2f%n", plugin.getKey(), plugin.getValue().getSum(), percentage);
        });

    out.println("-".repeat(60));
    out.printf("Number of default plugins used: %3d%n", DEFAULT_MAVEN_PLUGINS.size());
    out.println("-".repeat(60));
    collect.entrySet()
        .stream()
        .filter(k -> DEFAULT_MAVEN_PLUGINS.contains(k.getKey()))
        .forEach(plugin -> out.printf(" %-36s %,15d%n", plugin.getKey() , plugin.getValue().getSum()));

    out.println("-".repeat(60));
    var numberOfDownloadsSelectedPlugins = collect.entrySet()
        .stream()
        .filter(k -> DEFAULT_MAVEN_PLUGINS.contains(k.getKey()))
        .mapToLong(s -> s.getValue().getSum()).sum();

    out.printf(" %-36s %,15d%n", "numberOfDownloadsSelectedPlugins:", numberOfDownloadsSelectedPlugins);
  }

  @Test
  void start() throws IOException {
    Locale.setDefault(Locale.US);
    var rootDirectory = Paths.get("src/test/resources");

    mavenPluginStatistics(rootDirectory);
  }

}
