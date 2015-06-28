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

import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public class SparkClassLoader extends CombineClassLoader {

  private final ClassLoader programClassLoader;
  private final Configuration hConf;
  private final BasicSparkContext context;

  /**
   * Finds the SparkClassLoader from the context ClassLoader hierarchy.
   *
   * @return the SparkClassLoader found
   * @throws IllegalStateException if no SparkClassLoader was found
   */
  public static SparkClassLoader findFromContext() {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    SparkClassLoader sparkClassLoader = ClassLoaders.find(contextClassLoader,
                                                          SparkClassLoader.class);
    // Should found the Spark ClassLoader
    Preconditions.checkState(sparkClassLoader != null, "Cannot find SparkClassLoader from context ClassLoader %s",
                             contextClassLoader);
    return sparkClassLoader;
  }

  public SparkClassLoader(ClassLoader programClassLoader, Configuration hConf, BasicSparkContext context) {
    super(null, ImmutableList.of(programClassLoader, SparkClassLoader.class.getClassLoader()));
    this.programClassLoader = programClassLoader;
    this.hConf = hConf;
    this.context = context;
  }

  public Configuration getConfiguration() {
    return hConf;
  }

  public ClassLoader getProgramClassLoader() {
    return programClassLoader;
  }

  public BasicSparkContext getContext() {
    return context;
  }
}
