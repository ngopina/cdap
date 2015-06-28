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
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.internal.app.runtime.batch.dataset.DataSetOutputCommitter;
import co.cask.cdap.internal.app.runtime.spark.BasicSparkContext;
import co.cask.cdap.internal.app.runtime.spark.SparkClassLoader;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * An {@link OutputFormat} for writing into dataset.
 *
 * @param <KEY>   Type of key.
 * @param <VALUE> Type of value.
 *                TODO: Refactor this OutputFormat and MapReduce OutputFormat
 */
public final class SparkDatasetOutputFormat<KEY, VALUE> extends OutputFormat<KEY, VALUE> {

  private static final String OUTPUT_DATASET_NAME = "output.spark.dataset.name";

  public static void setDataset(Configuration configuration, String dataset) {
    configuration.set(OUTPUT_DATASET_NAME, dataset);
  }

  @Override
  public RecordWriter<KEY, VALUE> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    SparkClassLoader sparkClassLoader = SparkClassLoader.findFromContext();

    BasicSparkContext sparkContext = sparkClassLoader.getContext();
    BatchWritable<KEY, VALUE> batchWritable = getBatchWritable(context.getConfiguration().get(OUTPUT_DATASET_NAME),
                                                               sparkContext);

    // the record writer now owns the context and will close it
    return new DatasetRecordWriter<>(batchWritable, sparkContext);
  }


  @Override
  public void checkOutputSpecs(final JobContext context) throws IOException, InterruptedException {
    // TODO: validate out types? Or this is ensured by configuring job in "internal" code (i.e. not in user code)
  }

  @Override
  public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
    return new DataSetOutputCommitter();
  }


  /**
   * Returns a {@link BatchWritable} that is implemented by the given dataset.
   *
   * @param datasetName name of the dataset
   * @param sparkContext the {@link BasicSparkContext} for getting dataset
   * @param <K> key type for the BatchWritable
   * @param <V> value type for the BatchWritable
   * @return the BatchWritable
   * @throws DatasetInstantiationException if cannot find the dataset
   * @throws IllegalArgumentException if the dataset does not implement BatchWritable
   */
  @SuppressWarnings("unchecked")
  private <K, V> BatchWritable<K, V> getBatchWritable(String datasetName, BasicSparkContext sparkContext) {
    Dataset dataset = sparkContext.getDataset(datasetName);
    Preconditions.checkArgument(dataset instanceof BatchWritable, "Dataset %s of type %s does not implements %s",
                                datasetName, dataset.getClass().getName(), BatchWritable.class.getName());
    return (BatchWritable<K, V>) dataset;
  }
}
