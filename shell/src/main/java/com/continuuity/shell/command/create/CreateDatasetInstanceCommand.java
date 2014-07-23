/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.shell.command.create;

import com.continuuity.client.DatasetClient;
import com.continuuity.shell.command.AbstractCommand;
import com.continuuity.shell.completer.Completable;
import com.continuuity.shell.completer.reactor.DatasetTypeNameCompleter;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.List;
import javax.inject.Inject;

/**
 * Creates a dataset.
 */
public class CreateDatasetInstanceCommand extends AbstractCommand implements Completable {

  private final DatasetClient datasetClient;
  private final Completer completer;

  @Inject
  public CreateDatasetInstanceCommand(DatasetTypeNameCompleter completer, DatasetClient datasetClient) {
    super("instance", "<type-name> <new-dataset-name>", "Creates a dataset");
    this.completer = completer;
    this.datasetClient = datasetClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String datasetType = args[0];
    String datasetName = args[1];
    datasetClient.create(datasetName, datasetType);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, completer));
  }
}
