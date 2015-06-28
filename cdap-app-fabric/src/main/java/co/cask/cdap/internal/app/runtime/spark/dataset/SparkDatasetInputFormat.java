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

package co.cask.cdap.internal.app.runtime.spark.dataset;

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetInputSplit;
import co.cask.cdap.internal.app.runtime.spark.BasicSparkContext;
import co.cask.cdap.internal.app.runtime.spark.SparkClassLoader;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An {@link InputFormat} for {@link Spark} jobs that reads from {@link Dataset}.
 *
 * @param <KEY>   Type of key.
 * @param <VALUE> Type of value.
 *                TODO: Refactor this and MapReduce DatasetInputFormat
 */
public final class SparkDatasetInputFormat<KEY, VALUE> extends InputFormat<KEY, VALUE> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkDatasetInputFormat.class);

  // Set of job configuration keys for setting configurations into job conf.
  private static final String INPUT_DATASET_NAME = "input.spark.dataset.name";

  public static void setDataset(Configuration configuration, String dataset) {
    configuration.set(INPUT_DATASET_NAME, dataset);
  }

  @Override
  public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
    SparkClassLoader sparkClassLoader = SparkClassLoader.findFromContext();

    BatchReadable<?, ?> batchReadable = getBatchReadable(context.getConfiguration().get(INPUT_DATASET_NAME),
                                                         sparkClassLoader.getContext());
    List<Split> splits = batchReadable.getSplits();
    List<InputSplit> list = new ArrayList<>(splits.size());
    for (Split split : splits) {
      list.add(new DataSetInputSplit(split));
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<KEY, VALUE> createRecordReader(InputSplit split,
                                                     TaskAttemptContext context) throws IOException {
    SparkClassLoader sparkClassLoader = SparkClassLoader.findFromContext();
    DataSetInputSplit inputSplit = (DataSetInputSplit) split;

    final BasicSparkContext sparkContext = sparkClassLoader.getContext();
    final BatchReadable<KEY, VALUE> batchReadable = getBatchReadable(context.getConfiguration().get(INPUT_DATASET_NAME),
                                                                     sparkContext);
    final SplitReader<KEY, VALUE> splitReader = batchReadable.createSplitReader(inputSplit.getSplit());

    return new DatasetRecordReader<>(new SplitReader<KEY, VALUE>() {
      @Override
      public void initialize(Split split) throws InterruptedException {
        splitReader.initialize(split);
      }

      @Override
      public boolean nextKeyValue() throws InterruptedException {
        return splitReader.nextKeyValue();
      }

      @Override
      public KEY getCurrentKey() throws InterruptedException {
        return splitReader.getCurrentKey();
      }

      @Override
      public VALUE getCurrentValue() throws InterruptedException {
        return splitReader.getCurrentValue();
      }

      @Override
      public void close() {
        try {
          splitReader.close();
        } finally {
          commitAndClose();
        }
      }

      private void commitAndClose() {
        if (batchReadable instanceof Dataset) {
          try {
            sparkContext.commitAndClose((Dataset) batchReadable);
          } catch (Exception e) {
            // Nothing much can be done except propagating, which should make the spark job fail.
            // Log the exception as well since whether the Spark framework log it or not is out of our control.
            LOG.error("Failed to commit dataset %s", batchReadable, e);
            throw Throwables.propagate(e);
          }
        }
      }
    });
  }

  /**
   * Returns a {@link BatchReadable} that is implemented by the given dataset.
   *
   * @param datasetName name of the dataset
   * @param sparkContext the {@link BasicSparkContext} for getting dataset
   * @param <K> key type for the BatchReadable
   * @param <V> value type for the BatchReadable
   * @return the BatchReadable
   * @throws DatasetInstantiationException if cannot find the dataset
   * @throws IllegalArgumentException if the dataset does not implement BatchReadable
   */
  @SuppressWarnings("unchecked")
  private <K, V> BatchReadable<K, V> getBatchReadable(String datasetName, BasicSparkContext sparkContext) {
    Dataset dataset = sparkContext.getDataset(datasetName);
    Preconditions.checkArgument(dataset instanceof BatchReadable, "Dataset %s of type %s does not implements %s",
                                datasetName, dataset.getClass().getName(), BatchReadable.class.getName());
    return (BatchReadable<K, V>) dataset;
  }
}
