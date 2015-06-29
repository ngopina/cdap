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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PartitionedFileSetDatasetTest {

  @Test
  public void testGetDiff() throws Exception {
    long[] oldLongs = new long[]{1, 2, 3, 4, 5};
    long[] newLongs = new long[]{1};
    assertEquals(PartitionedFileSetDataset.getDiff(oldLongs, newLongs), 2L, 3L, 4L, 5L);

    oldLongs = new long[]{1, 2, 3, 4, 5};
    newLongs = new long[]{0, 1, 4, 6};
    assertEquals(PartitionedFileSetDataset.getDiff(oldLongs, newLongs), 2L, 3L, 5L);

    oldLongs = new long[]{0, 3, 6, 7, 8};
    newLongs = new long[]{1, 2, 3, 4, 5, 6, 7};
    assertEquals(PartitionedFileSetDataset.getDiff(oldLongs, newLongs), 0L, 8L);

    oldLongs = new long[]{};
    newLongs = new long[]{1, 2, 3, 4, 5, 6, 7};
    assertEquals(PartitionedFileSetDataset.getDiff(oldLongs, newLongs));

    oldLongs = new long[]{0, 3, 6, 7, 8};
    newLongs = new long[]{};
    assertEquals(PartitionedFileSetDataset.getDiff(oldLongs, newLongs), 0L, 3L, 6L, 7L, 8L);

    oldLongs = new long[]{};
    newLongs = new long[]{};
    assertEquals(PartitionedFileSetDataset.getDiff(oldLongs, newLongs));
  }

  private void assertEquals(List<?> a, Long... b) {
    Assert.assertEquals(a.size(), b.length);
    for (int i = 0; i < a.size(); i++) {
      Assert.assertEquals(a.get(i), b[i]);
    }
  }
}
