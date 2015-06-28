/*
/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.spark.metrics;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.spark.Spark;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.metrics.sink.Sink;
import org.apache.twill.filesystem.Location;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Sink} which collects Metrics from {@link Spark} program and used {@link SparkMetricsReporter} to report it
 * to {@link MetricsContext}
 * <p/>
 * This full qualified name of this class is given to spark through the metrics configuration file.
 */
public class SparkMetricsSink implements Sink {

  private static final int CONSOLE_DEFAULT_PERIOD = 1;
  private static final String CONSOLE_DEFAULT_UNIT = TimeUnit.SECONDS.name();

  private static final String CONSOLE_KEY_PERIOD = "period";
  private static final String CONSOLE_KEY_UNIT = "unit";

  private static final String SPARK_METRICS_SINK_NAME = "*.sink.console.class=";
  private static final String SPARK_METRICS_SINK_PERIOD = "*.sink.console.period=";
  private static final String SPARK_METRICS_SINK_UNIT = "*.sink.console.unit=";
  private static final String SPARK_METRICS_MASTER_PERIOD = "master.sink.console.period=";
  private static final String SPARK_METRICS_MASTER_UNIT = "master.sink.console.unit=";

  private final int pollPeriod;
  private final TimeUnit pollUnit;
  private final SparkMetricsReporter reporter;

  public SparkMetricsSink(Properties properties, MetricRegistry registry,
                          org.apache.spark.SecurityManager securityManager) {

    pollPeriod = properties.getProperty(CONSOLE_KEY_PERIOD).isEmpty() ?
      CONSOLE_DEFAULT_PERIOD :
      Integer.parseInt(properties.getProperty(CONSOLE_KEY_PERIOD));

    pollUnit = properties.getProperty(CONSOLE_KEY_UNIT).isEmpty() ?
      TimeUnit.valueOf(CONSOLE_DEFAULT_UNIT) :
      TimeUnit.valueOf(properties.getProperty(CONSOLE_KEY_UNIT).toUpperCase());

    MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod);

    reporter = new SparkMetricsReporter(registry,
                                        TimeUnit.SECONDS,
                                        TimeUnit.MILLISECONDS,
                                        MetricFilter.ALL);

  }

  @Override
  public void start() {
    reporter.start(pollPeriod, pollUnit);
  }

  @Override
  public void stop() {
    reporter.stop();
  }

  @Override
  public void report() {
    reporter.report();
  }

  /**
   * Generate a properties file which is used to config Spark Metrics in {@link SparkConf}
   *
   * @param file the {@link File} where this file should be generated
   * @return the same File argument provided.
   */
  public static File generateSparkMetricsConfig(File file) throws IOException {
    try (BufferedWriter writer = Files.newWriter(file, Charsets.UTF_8)) {
      writer.write(SPARK_METRICS_SINK_NAME);
      writer.write(SparkMetricsSink.class.getName());
      writer.newLine();
      writer.write(SPARK_METRICS_SINK_PERIOD);
      writer.write(String.valueOf(SparkMetricsSink.CONSOLE_DEFAULT_PERIOD));
      writer.newLine();
      writer.write(SPARK_METRICS_SINK_UNIT);
      writer.write(SparkMetricsSink.CONSOLE_DEFAULT_UNIT);
      writer.newLine();
      writer.write(SPARK_METRICS_MASTER_PERIOD);
      writer.write(String.valueOf(SparkMetricsSink.CONSOLE_DEFAULT_PERIOD));
      writer.newLine();
      writer.write(SPARK_METRICS_MASTER_UNIT);
      writer.write(SparkMetricsSink.CONSOLE_DEFAULT_UNIT);
      return file;
    }
  }
}
