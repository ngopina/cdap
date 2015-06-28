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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.ScalaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkProgram;
import com.google.common.base.Preconditions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which wraps around user's program class to integrate the spark program with CDAP.
 * This first command line argument to this class is the name of the user's Spark program class
 * followed by the arguments which will be passed to user's program class.
 * This Wrapper class is submitted to Spark and it does the following:
 * <ol>
 * <li>
 * Validates that there is at least {@link SparkProgramWrapper#PROGRAM_WRAPPER_ARGUMENTS_SIZE} command line arguments
 * </li>
 * <li>
 * Gets the user's program class through Spark's ExecutorURLClassLoader.
 * </li>
 * <li>
 * Sets {@link SparkContext} to concrete implementation of {@link JavaSparkContext} if user program implements {@link
 * JavaSparkProgram} or to {@link ScalaSparkContext} if user's program implements {@link ScalaSparkProgram}
 * </li>
 * <li>
 * Run user's program with extracted arguments from the argument list
 * </li>
 * </ol>
 */

public class SparkProgramWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProgramWrapper.class);
  private static final int PROGRAM_WRAPPER_ARGUMENTS_SIZE = 1;

//  // TODO: Get around Spark's limitation of only one SparkContext in a JVM and support multiple spark context:
//  // CDAP-4
//  private static boolean sparkProgramSuccessful;
//  private static boolean sparkProgramRunning;

  public static void main(String[] args) throws Exception {
    new SparkProgramWrapper(args);
  }

  /**
   * Constructor
   *
   * @param args the command line arguments
   * @throws RuntimeException if the user's program class is not found
   */
  @SuppressWarnings("unchecked")
  private SparkProgramWrapper(String[] args) throws Exception {
    String[] arguments = validateArgs(args);

    // Load the user class from the ProgramClassLoader
    Class<? extends SparkProgram> sparkProgramClass = loadUserSparkClass(arguments[0]);
    SparkContext sparkContext = getSparkContext(sparkProgramClass);
    sparkProgramClass.newInstance().run(sparkContext);
  }

  /**
   * Validates command line arguments being passed
   * Expects at least {@link SparkProgramWrapper#PROGRAM_WRAPPER_ARGUMENTS_SIZE} command line arguments to be present
   *
   * @param arguments String[] the arguments
   * @return String[] if the command line arguments are sufficient else throws a {@link RuntimeException}
   * @throws IllegalArgumentException if the required numbers of command line arguments were not present
   */
  private String[] validateArgs(String[] arguments) {
    if (arguments.length < PROGRAM_WRAPPER_ARGUMENTS_SIZE) {
      throw new IllegalArgumentException("Insufficient number of arguments. Program class name followed by its" +
                                           " arguments (if any) should be provided");
    }
    return arguments;
  }

  private SparkContext getSparkContext(Class<? extends SparkProgram> sparkProgramClass) {
    SparkClassLoader classLoader = SparkClassLoader.findFromContext();
    BasicSparkContext sparkContext = classLoader.getContext();

    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName(sparkContext.getProgramId().getId());
//    sparkConf.set(SPARK_METRICS_CONF_KEY, basicSparkContext.getMetricsPropertyFile().getAbsolutePath());

    if (JavaSparkProgram.class.isAssignableFrom(sparkProgramClass)) {
      sparkContext.setSparkFrameworkContext(new JavaSparkFrameworkContext(sparkConf));
    } else if (ScalaSparkProgram.class.isAssignableFrom(sparkProgramClass)) {
      sparkContext.setSparkFrameworkContext(new ScalaSparkFrameworkContext(sparkConf));
    } else {
      String error = "Spark program must implement either JavaSparkProgram or ScalaSparkProgram";
      throw new IllegalArgumentException(error);
    }

    return sparkContext;
  }

  @SuppressWarnings("unchecked")
  private Class<? extends SparkProgram> loadUserSparkClass(String className) throws ClassNotFoundException {
    SparkClassLoader classLoader = SparkClassLoader.findFromContext();
    Class<?> cls = classLoader.getProgramClassLoader().loadClass(className);
    Preconditions.checkArgument(SparkProgram.class.isAssignableFrom(cls),
                                "User class {} does not implements {}", className, SparkProgram.class);

    return (Class<? extends SparkProgram>) cls;
  }

//  /**
//   * Stops the Spark program by calling {@link org.apache.spark.SparkContext#stop()}
//   */
//  public static void stopSparkProgram() {
//    if (sparkContext != null) {
//      try {
//        if (isScalaProgram()) {
//          ((org.apache.spark.SparkContext) sparkContext.getOriginalSparkContext()).stop();
//        } else {
//          ((org.apache.spark.api.java.JavaSparkContext) sparkContext.getOriginalSparkContext()).stop();
//        }
//      } finally {
//        // Need to reset the spark context so that the program ClassLoader can be freed.
//        // It is needed because sparkContext is a static field.
//        // Same applies to basicSparkContext
//        sparkContext = null;
//        basicSparkContext = null;
//      }
//    }
//  }

//  /**
//   * @param sparkProgramSuccessful a boolean to which the programSuccess status will be set to
//   */
//  public static void setSparkProgramSuccessful(boolean sparkProgramSuccessful) {
//    SparkProgramWrapper.sparkProgramSuccessful = sparkProgramSuccessful;
//  }
}
