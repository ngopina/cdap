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

package com.continuuity.shell.command.get;

import com.continuuity.client.ProgramClient;
import com.continuuity.shell.ProgramElementType;
import com.continuuity.shell.ProgramIdCompleterFactory;
import com.continuuity.shell.command.AbstractCommand;
import com.continuuity.shell.completer.Completable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.List;

/**
 * Gets the logs of a program.
 */
public class GetProgramLogsCommand extends AbstractCommand implements Completable {

  private final ProgramClient programClient;
  private final ProgramIdCompleterFactory completerFactory;
  private final ProgramElementType programElementType;

  protected GetProgramLogsCommand(ProgramElementType programElementType,
                                  ProgramIdCompleterFactory completerFactory,
                                  ProgramClient programClient) {
    super(programElementType.getName(), "<app-id>.<program-id> [<start-time> <end-time>]",
          "Gets the logs of a " + programElementType.getName());
    this.programElementType = programElementType;
    this.completerFactory = completerFactory;
    this.programClient = programClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String[] programIdParts = args[0].split("\\.");
    String appId = programIdParts[0];

    long start;
    long stop;
    if (args.length >= 4) {
      start = Long.parseLong(args[2]);
      stop = Long.parseLong(args[3]);
      Preconditions.checkArgument(stop >= start, "stop timestamp must be greater than start timestamp");
    } else {
      start = 0;
      stop = Long.MAX_VALUE;
    }

    String logs;
    if (programElementType == ProgramElementType.RUNNABLE) {
      String serviceId = programIdParts[1];
      String runnableId = programIdParts[2];
      logs = programClient.getServiceRunnableLogs(appId, serviceId, runnableId, start, stop);
    } else if (programElementType.getProgramType() != null) {
      String programId = programIdParts[1];
      logs = programClient.getProgramLogs(appId, programElementType.getProgramType(), programId, start, stop);
    } else {
      throw new IllegalArgumentException("Cannot get logs for " + programElementType.getPluralName());
    }

    output.println(logs);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, completerFactory.getProgramIdCompleter(programElementType)));
  }
}
