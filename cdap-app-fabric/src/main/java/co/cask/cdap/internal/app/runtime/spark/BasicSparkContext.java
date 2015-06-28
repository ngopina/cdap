/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.stream.StreamEventDecoder;
import co.cask.cdap.data.dataset.DatasetInstantiator;
import co.cask.cdap.data.stream.StreamInputFormat;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.internal.app.runtime.spark.dataset.SparkDatasetInputFormat;
import co.cask.cdap.proto.Id;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Spark job runtime context. This context serves as the bridge between CDAP {@link SparkProgramRunner} and Spark
 * programs running in YARN containers. The {@link SparkProgramRunner} builds this context and writes it to {@link
 * Configuration} through {@link SparkContextConfig}. The running Spark jobs loads the {@link Configuration} files
 * which was packaged with the dependency jar when the job was submitted and constructs this context back through
 * {@link SparkContextProvider}. This allow Spark jobs running outside CDAP have access to Transaction,
 * start time and other stuff.
 */
public class BasicSparkContext implements SparkContext {

  private static final Logger LOG = LoggerFactory.getLogger(BasicSparkContext.class);

  // A Regex pattern that split with whitespace that supports "" quotation.
  private static final Pattern QUOTED_STRING_PATTERN = Pattern.compile("([^\"]\\S*|\".+?\")\\s*");

  private final Configuration hConf;
  private final SparkSpecification specification;
  private final Id.Program programId;
  private final RunId runId;
  private final long logicalStartTime;
  private final Map<String, String> runtimeArguments;
  private final DatasetInstantiator datasetInstantiator;
  private final StreamAdmin streamAdmin;

  private SparkFrameworkContext sparkFrameworkContext;
  private Transaction transaction;

  public BasicSparkContext(Configuration hConf, SparkSpecification specification,
                           Id.Program programId, RunId runId, ClassLoader classLoader,
                           long logicalStartTime, Map<String, String> runtimeArguments,
                           DatasetFramework datasetFramework, StreamAdmin streamAdmin, MetricsContext metricsContext) {
    this.hConf = hConf;
    this.specification = specification;
    this.programId = programId;
    this.runId = runId;
    this.logicalStartTime = logicalStartTime;
    this.runtimeArguments = ImmutableMap.copyOf(runtimeArguments);
    this.streamAdmin = streamAdmin;
    this.datasetInstantiator = new DatasetInstantiator(programId.getNamespace(), datasetFramework,
                                                       classLoader, getOwners(programId), metricsContext);
  }

  public void setSparkFrameworkContext(SparkFrameworkContext sparkFrameworkContext) {
    this.sparkFrameworkContext = sparkFrameworkContext;
  }

  public void setTransaction(Transaction transaction) {
    this.transaction = transaction;
  }

  public Transaction getTransaction() {
    return transaction;
  }

  public Id.Program getProgramId() {
    return programId;
  }

  public RunId getRunId() {
    return runId;
  }

  @Override
  public SparkSpecification getSpecification() {
    return specification;
  }

  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }

  @Override
  public <T> T readFromDataset(String datasetName, Class<?> kClass, Class<?> vClass) {
    Preconditions.checkState(sparkFrameworkContext != null,
                             "SparkContext not available in the current execution context.");

    Configuration configuration = new Configuration(hConf);
    SparkDatasetInputFormat.setDataset(configuration, datasetName);
    return sparkFrameworkContext.createRDD(SparkDatasetInputFormat.class, kClass, vClass, configuration);
  }

  @Override
  public <T> void writeToDataset(T rdd, String datasetName, Class<?> kClass, Class<?> vClass) {
    Preconditions.checkState(sparkFrameworkContext != null,
                             "SparkContext not available in the current execution context.");
    sparkFrameworkContext.saveAsDataset(rdd, datasetName, kClass, vClass, new Configuration(hConf));
  }


  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass) {
    return readFromStream(streamName, vClass, 0, System.currentTimeMillis());
  }

  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass, long startTime, long endTime) {
    return readFromStream(streamName, vClass, startTime, endTime, null);
  }

  @Override
  public <T> T readFromStream(String streamName, Class<?> vClass,
                              long startTime, long endTime, Class<? extends StreamEventDecoder> decoderType) {
    Preconditions.checkState(sparkFrameworkContext != null,
                             "SparkContext not available in the current execution context.");

    StreamBatchReadable stream = (decoderType == null)
      ? new StreamBatchReadable(streamName, startTime, endTime)
      : new StreamBatchReadable(streamName, startTime, endTime, decoderType);

    try {
      Configuration configuration = configureStreamInput(new Configuration(hConf), stream, vClass);
      T streamRDD = sparkFrameworkContext.createRDD(StreamInputFormat.class,
                                                    LongWritable.class, vClass, configuration);

      // Register for stream usage for the Spark program
      Id.Stream streamId = Id.Stream.from(programId.getNamespace(), streamName);
      List<? extends Id> owners = getOwners(programId);
      try {
        streamAdmin.register(owners, streamId);
      } catch (Exception e) {
        LOG.warn("Failed to registry usage of {} -> {}", streamId, owners, e);
      }
      return streamRDD;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public <T> T getOriginalSparkContext() {
    Preconditions.checkState(sparkFrameworkContext != null,
                             "SparkContext not available in the current execution context.");
    return sparkFrameworkContext.getContext();
  }

  @Override
  public String[] getRuntimeArguments(String argsKey) {
    String value = getRuntimeArguments().get(argsKey);
    if (value == null) {
      return new String[0];
    }

    // Split the argument values with whitespace, with quotation supported.
    List<String> args = new ArrayList<>();
    Matcher matcher = QUOTED_STRING_PATTERN.matcher(value);
    while (matcher.find()) {
      args.add(matcher.group(1));
    }

    return args.toArray(new String[args.size()]);
  }

  @Override
  public ServiceDiscoverer getServiceDiscoverer() {
    // TODO: Need a serializable ServiceDiscover
    throw new UnsupportedOperationException();
  }

  @Override
  public Metrics getMetrics() {
    // TODO: Needs a serializable Metrics
    throw new UnsupportedOperationException();
  }

  public void commitAndClose(Dataset dataset) throws Exception {
    try {
      if (dataset instanceof TransactionAware) {
        ((TransactionAware) dataset).commitTx();
      }
    } finally {
      if (dataset instanceof TransactionAware) {
        datasetInstantiator.removeTransactionAware((TransactionAware) dataset);
      }
      Closeables.closeQuietly(dataset);
    }
  }

  private Configuration configureStreamInput(Configuration configuration,
                                             StreamBatchReadable stream, Class<?> vClass) throws IOException {

    Id.Stream streamId = Id.Stream.from(programId.getNamespace(), stream.getStreamName());

    StreamConfig streamConfig = streamAdmin.getConfig(streamId);
    Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                               StreamUtils.getGeneration(streamConfig));
    StreamInputFormat.setTTL(configuration, streamConfig.getTTL());
    StreamInputFormat.setStreamPath(configuration, streamPath.toURI());
    StreamInputFormat.setTimeRange(configuration, stream.getStartTime(), stream.getEndTime());

    String decoderType = stream.getDecoderType();
    if (decoderType == null) {
      // If the user don't specify the decoder, detect the type
      StreamInputFormat.inferDecoderClass(configuration, vClass);
    } else {
      StreamInputFormat.setDecoderClassName(configuration, decoderType);
    }
    return configuration;
  }



  // TODO: The following methods have similar implementation as in AbstractContext
  // Need refactoring to make AbstractContext less bulky first before able to reduce code duplication
  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
    return getDataset(name, RuntimeArguments.NO_ARGUMENTS);
  }

  public <T extends Dataset> T getDataset(String name,
                                          Map<String, String> arguments) throws DatasetInstantiationException {
    // TODO this should allow to get a dataset that was not declared with @UseDataSet. Then we can support arguments.
    try {
      T dataset = datasetInstantiator.getDataset(name, arguments);
      if (dataset == null) {
        throw new DatasetInstantiationException(String.format("'%s' is not a known Dataset", name));
      }
      if (dataset instanceof TransactionAware) {
        ((TransactionAware) dataset).startTx(transaction);
      }
      return dataset;
    } catch (Throwable t) {
      throw new DatasetInstantiationException(String.format("Can't instantiate dataset '%s'", name), t);
    }
  }

  private List<? extends Id> getOwners(Id.Program programId) {
    return ImmutableList.of(programId);
  }
}
