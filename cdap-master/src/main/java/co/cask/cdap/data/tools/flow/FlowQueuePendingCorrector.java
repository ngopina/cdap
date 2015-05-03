/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.data.tools.flow;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsCollector;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.queue.QueueSpecification;
import co.cask.cdap.app.queue.QueueSpecificationGenerator;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.TwillModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data.runtime.DataFabricDistributedModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.tools.HBaseQueueDebugger;
import co.cask.cdap.data2.queue.QueueClientFactory;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueAdmin;
import co.cask.cdap.data2.transaction.queue.hbase.HBaseQueueClientFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.internal.app.queue.SimpleQueueSpecificationGenerator;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Corrects the "queue.pending" metric emitted in {@link co.cask.cdap.internal.app.runtime.flow.FlowletProgramRunner}.
 */
public class FlowQueuePendingCorrector extends AbstractIdleService {

  private static final Gson GSON = new Gson();

  private final MetricsCollectionService metricsCollectionService;
  private final MetricStore metricStore;
  private final KafkaClientService kafkaClientService;
  private final HBaseQueueDebugger queueDebugger;
  private final ZKClientService zkClientService;
  private final Store store;
  private final ProgramRuntimeService programRuntimeService;
  private final TwillRunnerService twillRunnerService;

  @Inject
  public FlowQueuePendingCorrector(HBaseQueueDebugger queueDebugger, ZKClientService zkClientService,
                                   MetricsCollectionService metricsCollectionService, MetricStore metricStore,
                                   KafkaClientService kafkaClientService, Store store,
                                   ProgramRuntimeService programRuntimeService,
                                   TwillRunnerService twillRunnerService) {
    this.queueDebugger = queueDebugger;
    this.zkClientService = zkClientService;
    this.metricsCollectionService = metricsCollectionService;
    this.metricStore = metricStore;
    this.kafkaClientService = kafkaClientService;
    this.store = store;
    this.programRuntimeService = programRuntimeService;
    this.twillRunnerService = twillRunnerService;
  }

  public void run(Id.Flow flowId) throws Exception {
    System.out.println("Running queue.pending correction on flow " + flowId);

    SimpleQueueSpecificationGenerator queueSpecGenerator =
      new SimpleQueueSpecificationGenerator(flowId.getApplication());

    ApplicationSpecification app = store.getApplication(flowId.getApplication());
    Preconditions.checkArgument(app != null);
    for (FlowSpecification flow : app.getFlows().values()) {
      Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> table = queueSpecGenerator.create(flow);
      for (Table.Cell<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> cell : table.cellSet()) {
        if (cell.getRowKey().getType() == FlowletConnection.Type.FLOWLET) {
          String producerFlowlet = cell.getRowKey().getName();
          String consumerFlowlet = cell.getColumnKey();
          for (QueueSpecification queue : cell.getValue()) {
            run(flowId, producerFlowlet, consumerFlowlet, queue.getQueueName().getSimpleName());
          }
        }
      }
    }
  }

  public void run(Id.Flow flowId, String producerFlowlet, String consumerFlowlet,
                  String flowletQueue) throws Exception {

    System.out.println("Running queue.pending correction on flow '" + flowId + "' producerFlowlet '" + producerFlowlet
                         + "' consumerFlowlet '" + consumerFlowlet + "' flowletQueue '" + flowletQueue + "'");
    Map<RunId, ProgramRuntimeService.RuntimeInfo> runtimeInfos = programRuntimeService.list(flowId);
    Preconditions.checkState(runtimeInfos.isEmpty(), "Cannot run tool when flow " + flowId + " is still running");

    ApplicationSpecification app = store.getApplication(flowId.getApplication());
    Preconditions.checkArgument(app != null, flowId.getApplication() + " not found");
    Preconditions.checkArgument(app.getFlows().containsKey(flowId.getId()), flowId + " not found");

    FlowSpecification flow = app.getFlows().get(flowId.getId());
    SimpleQueueSpecificationGenerator queueSpecGenerator =
      new SimpleQueueSpecificationGenerator(flowId.getApplication());
    Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> table = queueSpecGenerator.create(flow);

    Preconditions.checkArgument(
      table.contains(QueueSpecificationGenerator.Node.flowlet(producerFlowlet), consumerFlowlet),
      "Flowlet " + producerFlowlet + " is not emitting to " + consumerFlowlet);
    Set<QueueSpecification> queueSpecs =
      table.get(QueueSpecificationGenerator.Node.flowlet(producerFlowlet), consumerFlowlet);
    boolean validQueue = false;
    for (QueueSpecification queueSpec : queueSpecs) {
      if (queueSpec.getQueueName().getSimpleName().equals(flowletQueue)) {
        validQueue = true;
        break;
      }
    }
    Preconditions.checkArgument(validQueue, "Queue " + flowletQueue + " does not exist for the given flowlets");

    QueueName queueName = QueueName.fromFlowlet(flowId, producerFlowlet, flowletQueue);
    long consumerGroupId = FlowUtils.generateConsumerGroupId(flowId, consumerFlowlet);
    HBaseQueueDebugger.QueueStatistics stats = queueDebugger.scanQueue(queueName, consumerGroupId);
    long correctQueuePendingValue = stats.getUnprocessed() + stats.getProcessedAndNotVisible();

    Map<String, String> tags = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.NAMESPACE, flowId.getNamespaceId())
      .put(Constants.Metrics.Tag.APP, flowId.getApplicationId())
      .put(Constants.Metrics.Tag.FLOW, flowId.getId())
      .put(Constants.Metrics.Tag.CONSUMER, consumerFlowlet)
      .put(Constants.Metrics.Tag.PRODUCER, producerFlowlet)
      .put(Constants.Metrics.Tag.FLOWLET_QUEUE, flowletQueue)
      .build();

    MetricDataQuery query = new MetricDataQuery(
      0, 0, Integer.MAX_VALUE, 1, ImmutableMap.of("system.queue.pending", AggregationFunction.SUM),
      tags, ImmutableList.<String>of(), null);

    Collection<MetricTimeSeries> results = metricStore.query(query);
    long queuePending;
    if (results.isEmpty()) {
      queuePending = 0;
    } else {
      System.out.println("Got results: " + GSON.toJson(results));
      Preconditions.checkState(results.size() == 1);
      List<TimeValue> timeValues = results.iterator().next().getTimeValues();
      Preconditions.checkState(timeValues.size() == 1);
      TimeValue timeValue = timeValues.get(0);
      queuePending = timeValue.getValue();
    }

    MetricsCollector collector = metricsCollectionService.getCollector(tags);
    collector.gauge("queue.pending", correctQueuePendingValue);
    System.out.printf("Adjusted system.queue.pending metric from %d to %d (tags %s)\n",
                      queuePending, correctQueuePendingValue, GSON.toJson(tags));
  }

  @Override
  protected void startUp() throws Exception {
    kafkaClientService.startAndWait();
    zkClientService.startAndWait();
    metricsCollectionService.startAndWait();
    twillRunnerService.startAndWait();
    programRuntimeService.startAndWait();
    queueDebugger.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    queueDebugger.stopAndWait();
    programRuntimeService.startAndWait();
    twillRunnerService.startAndWait();
    // stop will flush the metrics
    metricsCollectionService.stopAndWait();
    zkClientService.stopAndWait();
    kafkaClientService.stopAndWait();
  }

  public static FlowQueuePendingCorrector createCorrector() {
    Injector injector = Guice.createInjector(
      new IOModule(),
      new ConfigModule(CConfiguration.create(), HBaseConfiguration.create()),
      new ZKClientModule(),
      new TwillModule(),
      new KafkaClientModule(),
      new DataFabricDistributedModule(),
      new NotificationFeedClientModule(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(Store.class).to(DefaultStore.class);
          bind(QueueClientFactory.class).to(HBaseQueueClientFactory.class).in(Singleton.class);
          bind(QueueAdmin.class).to(HBaseQueueAdmin.class).in(Singleton.class);
          bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);
        }
      }
    );

    return injector.getInstance(FlowQueuePendingCorrector.class);
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArgs(args);
    FlowQueuePendingCorrector corrector = createCorrector();
    corrector.startAndWait();

    try {
      String namespace = cmd.getOptionValue("namespace");
      String app = cmd.getOptionValue("app");
      String flow = cmd.getOptionValue("flow");

      if (cmd.hasOption("producer-flowlet")) {
        Preconditions.checkArgument(cmd.hasOption("consumer-flowlet"), "Missing consumer-flowlet option");
        String producerFlowlet = cmd.getOptionValue("producer-flowlet");
        String consumerFlowlet = cmd.getOptionValue("consumer-flowlet");
        String queue = cmd.getOptionValue("queue", "queue");
        corrector.run(Id.Flow.from(namespace, app, flow), producerFlowlet, consumerFlowlet, queue);
      } else {
        corrector.run(Id.Flow.from(namespace, app, flow));
      }
    } finally {
      corrector.stopAndWait();
    }
  }

  private static CommandLine parseArgs(String[] args) {
    Options options = new Options();
    options.addOption(createOption("n", "namespace", true, "namespace", true));
    options.addOption(createOption("a", "app", true, "app", true));
    options.addOption(createOption("f", "flow", true, "flow", true));
    options.addOption(createOption("p", "producer-flowlet", true,
                                   "producer flowlet (optional, leave empty to correct entire flow)", false));
    options.addOption(createOption("c", "consumer-flowlet", true,
                                   "consumer flowlet (optional, leave empty to correct entire flow)", false));
    options.addOption(createOption("q", "queue", true, "flowlet queue (optional, defaults to \"queue\")", false));

    CommandLineParser parser = new BasicParser();
    try {
      return parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      String argsFormat =
        "--namespace <namespace> " +
          "--app <app> " +
          "--flow <flow> " +
          "[--producer-flowlet <flowlet> " +
          "--consumer-flowlet <flowlet> " +
          "[--queue <queue>]]";
      formatter.printHelp(argsFormat, options);
      System.exit(0);
      return null;
    }
  }

  private static Option createOption(String opt, String longOpt, boolean hasOpt,
                                     String desc, boolean required) {
    Option option = new Option(opt, longOpt, hasOpt, desc);
    if (required) {
      option.setRequired(true);
    }
    return option;
  }
}