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
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 *
 */
public class ScalaSparkFrameworkContext implements SparkFrameworkContext {

  private final SparkContext sparkContext;

  public ScalaSparkFrameworkContext(SparkConf sparkConf) {
    this.sparkContext = new SparkContext(sparkConf);
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
                                               Class<? extends OutputFormat<K, V>> outputFormatClass,
                                               Class<K> keyClass, Class<V> valueClass, Configuration hConf) {
    Preconditions.checkArgument(rdd instanceof RDD,
                                "RDD class %s is not a subclass of %s",
                                rdd.getClass().getName(), RDD.class.getName());

    ClassTag<K> kClassTag = ClassTag$.MODULE$.apply(keyClass);
    ClassTag<V> vClassTag = ClassTag$.MODULE$.apply(valueClass);

    PairRDDFunctions<K, V> pairRDD = new PairRDDFunctions<K, V>((RDD<Tuple2<K, V>>) rdd, kClassTag, vClassTag, null);
    pairRDD.saveAsNewAPIHadoopFile(name, keyClass, valueClass, outputFormatClass, hConf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getContext() {
    return (T) sparkContext;
  }
}
