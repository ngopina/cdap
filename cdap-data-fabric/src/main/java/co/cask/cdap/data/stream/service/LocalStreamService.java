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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.util.Map;

/**
 * Stream service running in local mode.
 */
public class LocalStreamService extends AbstractStreamService {

  private final StreamAdmin streamAdmin;
  private final StreamWriterSizeCollector streamWriterSizeCollector;
  private final StreamWriterSizeFetcher streamWriterSizeFetcher;
  private final StreamMetaStore streamMetaStore;
  private final Map<String, Long> streamsBaseSizes;
  private boolean isInit;

  @Inject
  public LocalStreamService(StreamCoordinatorClient streamCoordinatorClient,
                            StreamFileJanitorService janitorService,
                            StreamMetaStore streamMetaStore,
                            StreamAdmin streamAdmin,
                            StreamWriterSizeCollector streamWriterSizeCollector,
                            StreamWriterSizeFetcher streamWriterSizeFetcher) {
    super(streamCoordinatorClient, janitorService);
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;
    this.streamWriterSizeCollector = streamWriterSizeCollector;
    this.streamWriterSizeFetcher = streamWriterSizeFetcher;
    this.streamsBaseSizes = Maps.newHashMap();
    this.isInit = true;
  }

  @Override
  protected void initialize() throws Exception {
    // No-op
  }

  @Override
  protected void doShutdown() throws Exception {
    // No-op
  }

  @Override
  protected void runOneIteration() throws Exception {
    // Get stream size - which will be the entire size - and send a notification if the size is big enough
    for (StreamSpecification streamSpec : streamMetaStore.listStreams()) {
      Long baseSize = streamsBaseSizes.get(streamSpec.getName());
      if (baseSize == null) {
        // First time that this stream is called in this method
        baseSize = streamWriterSizeFetcher.fetchSize(streamAdmin.getConfig(streamSpec.getName()));
        streamsBaseSizes.put(streamSpec.getName(), baseSize);
      }

      long absoluteSize = baseSize + streamWriterSizeCollector.getTotalCollected(streamSpec.getName());

      // TODO check that this size is higher than a threshold, and send a notification is so - or if isInit is true too
      // TODO will come in a later PR

    }
    isInit = false;
  }
}
