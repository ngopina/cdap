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

package co.cask.cdap.api;

import co.cask.cdap.api.data.stream.StreamWriter;
import co.cask.cdap.api.templates.AdapterContext;

/**
 * Defines an interface for Runnable components which need access to datasets, services, streams.
 */
public interface RunnableContext extends RuntimeContext, ServiceDiscoverer, StreamWriter, AdapterContext {

  /**
   * Execute a set of operations on datasets via a {@link TxRunnable} that are committed as a single transaction.
   * @param txRunnable the runnable to be executed in the transaction
   */
  void execute(TxRunnable txRunnable);

  /**
   * @return the instance id
   */
  int getInstanceId();
}
