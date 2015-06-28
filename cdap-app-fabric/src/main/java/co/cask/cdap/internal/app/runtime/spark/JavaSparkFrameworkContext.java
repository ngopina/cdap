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

package co.cask.cdap.internal.app.runtime.spark;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 */
public class JavaSparkFrameworkContext implements SparkFrameworkContext {

  private final org.apache.spark.api.java.JavaSparkContext sparkContext;

  public JavaSparkFrameworkContext(SparkConf sparkConf) {
    this.sparkContext = new JavaSparkContext(sparkConf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R, K, V> R newAPIHadoopFile(String name, Class<? extends InputFormat> inputFormatClass,
                                      Class<K> keyClass, Class<V> valueClass, Configuration hConf) {
    return (R) sparkContext.newAPIHadoopFile(name, inputFormatClass, keyClass, valueClass, hConf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R, K, V> void saveAsNewAPIHadoopFile(R rdd, String name,
                                               Class<? extends OutputFormat> outputFormatClass,
                                               Class<K> keyClass, Class<V> valueClass, Configuration hConf) {
    Preconditions.checkArgument(rdd instanceof JavaPairRDD,
                                "RDD class %s is not a subclass of %s",
                                rdd.getClass().getName(), JavaPairRDD.class.getName());
    ((JavaPairRDD<K, V>) rdd).saveAsNewAPIHadoopFile(name, keyClass, valueClass, outputFormatClass, hConf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getContext() {
    return (T) sparkContext;
  }
}
