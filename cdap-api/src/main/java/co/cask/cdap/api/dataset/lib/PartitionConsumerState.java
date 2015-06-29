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

package co.cask.cdap.api.dataset.lib;

import javax.annotation.Nullable;

/**
 * Contains the state necessary to keep track of which partitions are processed and which partitions would need to be
 * processed as they are created.
 */
public class PartitionConsumerState {
  // useful on initial query of partitions
  public static final PartitionConsumerState FROM_BEGINNING = new PartitionConsumerState(null, new long[0]);

  // Write pointer of the transaction from the previous query of partitions. This is used to scan for new partitions
  // created since then.
  private final Long startVersion;
  // The list of in progress transactions from the previous query of partitions. This is necessary because these might
  // be creations of partitions that fall before the startVersion.
  private final long[] versionsToCheck;

  public PartitionConsumerState(@Nullable Long startVersion, long[] versionsToCheck) {
    this.startVersion = startVersion;
    this.versionsToCheck = versionsToCheck;
  }

  @Nullable
  public Long getStartVersion() {
    return startVersion;
  }

  public long[] getVersionsToCheck() {
    return versionsToCheck;
  }
}
