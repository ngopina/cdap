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

package co.cask.cdap.internal.app.runtime.batch.distributed;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.runtime.batch.AbstractMapReduceContextBuilder;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.kafka.KafkaLogAppender;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;

/**
 * Builds an instance of {@link co.cask.cdap.internal.app.runtime.batch.BasicMapReduceContext} good for
 * distributed environment. The context is to be used in remote worker (e.g. Mapper task started in YARN container)
 */
public class DistributedMapReduceContextBuilder extends AbstractMapReduceContextBuilder {
  private final CConfiguration cConf;
  private final Configuration hConf;
  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private MetricsCollectionService metricsCollectionService;

  public DistributedMapReduceContextBuilder(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  @Override
  protected Injector prepare() {
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new ExploreClientModule(),
      new DataSetsModules().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // Data-fabric bindings
          bind(HBaseTableUtil.class).toProvider(HBaseTableUtilFactory.class);
          // For log publishing
          bind(LogAppender.class).to(KafkaLogAppender.class);
        }
      }
    );

    zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    kafkaClientService = injector.getInstance(KafkaClientService.class);
    kafkaClientService.startAndWait();

    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    metricsCollectionService.startAndWait();

    // Initialize log appender in distributed mode.
    // In single node the log appender is initialized during process startup.
    LogAppenderInitializer logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();

    return injector;
  }

  @Override
  protected void finish() {
    metricsCollectionService.stop();
    kafkaClientService.stop();
    zkClientService.stop();
  }
}
