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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * Interface to provide abstraction for accessing either scala or java version of the Apache SparkContext.
 */
public interface SparkFrameworkContext {

  <R, K, V> R newAPIHadoopFile(String name, Class<? extends InputFormat> inputFormatClass,
                             Class<K> keyClass, Class<V> valueClass, Configuration hConf);

  <R, K, V> void saveAsNewAPIHadoopFile(R rdd, String name, Class<? extends OutputFormat<K, V>> outputFormatClass,
                                        Class<K> keyClass, Class<V> valueClass, Configuration hConf);

  <T> T getContext();
}
