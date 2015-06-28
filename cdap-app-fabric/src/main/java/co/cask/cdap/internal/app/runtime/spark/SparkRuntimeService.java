/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.utils.ApplicationBundler;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.spark.deploy.SparkSubmit;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.Executor;
import java.util.jar.JarOutputStream;

/**
 * Performs the actual execution of Spark job.
 * <p/>
 * Service start -> Performs job setup, beforeSubmit and submit job
 * Service run -> Submits the spark job through {@link SparkSubmit}
 * Service triggerStop -> kill job
 * Service stop -> Commit/invalidate transaction, onFinish, cleanup
 */
final class SparkRuntimeService extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeService.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final Spark spark;
  private final SparkSpecification sparkSpecification;
  private final Location programJarLocation;
  private final BasicSparkContext context;
  private final TransactionSystemClient txClient;
  private Transaction transaction;
  private Runnable cleanupTask;
  private String[] sparkSubmitArgs;
  private volatile boolean stopRequested;
  private boolean success;

  SparkRuntimeService(CConfiguration cConf, Configuration hConf, Spark spark, SparkSpecification sparkSpecification,
                      BasicSparkContext context, Location programJarLocation, TransactionSystemClient txClient) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.spark = spark;
    this.sparkSpecification = sparkSpecification;
    this.programJarLocation = programJarLocation;
    this.context = context;
    this.txClient = txClient;
  }

  @Override
  protected String getServiceName() {
    return "Spark - " + sparkSpecification.getName();
  }

  @Override
  protected void startUp() throws Exception {
    // additional spark job initialization at run-time
    beforeSubmit();

    File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                   cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    this.cleanupTask = createCleanupTask(tempDir);
    try {
      File programJarCopy = copyProgramJar(programJarLocation, tempDir);
      File dependencyJar = buildDependencyJar(tempDir);
      File dummyJar = generateDummyJar(tempDir);
      File hConfFile = saveHConf(hConf, tempDir);

      // Generate and setup the metrics configuration for spark
//      context.setMetricsPropertyFile(
//        SparkMetricsSink.generateSparkMetricsConfig(File.createTempFile("spark.metrics", ".properties", tempDir)));

      // Create a long running transaction
      transaction = txClient.startLong();
      context.setTransaction(transaction);
//      SparkContextConfig contextConfig = new SparkContextConfig(new Configuration(hConf));
//      contextConfig.set(context, cConf, transaction, );

      sparkSubmitArgs = prepareSparkSubmitArgs(sparkSpecification, hConf,
                                               programJarCopy, dependencyJar, dummyJar, hConfFile);

    } catch (Throwable t) {
      cleanupTask.run();
      throw t;
    }
  }

  @Override
  protected void run() throws Exception {
    SparkClassLoader sparkClassLoader = new SparkClassLoader(spark.getClass().getClassLoader(), hConf, context);
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(sparkClassLoader);
    try {
      SparkSubmit.main(sparkSubmitArgs);
      success = true;
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }
//
//    // If the job is not successful, throw exception so that this service will terminate with a failure state
//    // Shutdown will still get executed, but the service will notify failure after that.
//    // However, if it's the job is requested to stop (via triggerShutdown, meaning it's a user action), don't throw
//    if (!stopRequested) {
//      // if spark program is not running anymore and it was successful we can say the the program succeeded
//      boolean programStatus = (!SparkProgramWrapper.isSparkProgramRunning()) &&
//        SparkProgramWrapper.isSparkProgramSuccessful();
//      Preconditions.checkState(programStatus, "Spark program execution failed.");
//    }
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      if (success) {
        LOG.info("Committing Spark Program transaction: {}", context);
        if (!txClient.commit(transaction)) {
          LOG.warn("Spark Job transaction failed to commit");
          throw new TransactionFailureException("Failed to commit transaction for Spark " + context.toString());
        }
      } else {
        // invalidate the transaction as spark might have written to datasets too
        txClient.invalidate(transaction.getWritePointer());
      }
    } finally {
      // whatever happens we want to call this
      try {
        onFinish(success);
      } finally {
        cleanupTask.run();
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    try {
      stopRequested = true;
      // TODO
//      if (SparkProgramWrapper.isSparkProgramRunning()) {
//        SparkProgramWrapper.stopSparkProgram();
//      }
    } catch (Exception e) {
      LOG.error("Failed to stop Spark job {}", sparkSpecification.getName(), e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected Executor executor() {
    // Always execute in new daemon thread.
    return new Executor() {
      @Override
      public void execute(final Runnable runnable) {
        final Thread t = new Thread(new Runnable() {

          @Override
          public void run() {
            // note: this sets logging context on the thread level
//            LoggingContextAccessor.setLoggingContext(context.getLoggingContext());
            runnable.run();
          }
        });
        t.setDaemon(true);
        t.setName(getServiceName());
        t.start();
      }
    };
  }

  /**
   * Calls the {@link Spark#beforeSubmit(SparkContext)} method.
   */
  private void beforeSubmit() throws TransactionFailureException, InterruptedException {
    createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(spark.getClass().getClassLoader());
        try {
          spark.beforeSubmit(context);
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }
      }
    });
  }

  /**
   * Calls the {@link Spark#onFinish(boolean, SparkContext)} method.
   */
  private void onFinish(final boolean succeeded) throws TransactionFailureException, InterruptedException {
    createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        spark.onFinish(succeeded, context);
      }
    });
  }

  /**
   * Creates a {@link TransactionExecutor} with all the {@link co.cask.tephra.TransactionAware} in the context.
   */
  private TransactionExecutor createTransactionExecutor() {
//    return new DefaultTransactionExecutor(txClient, context.getDatasetInstantiator().getTransactionAware());
    return new DefaultTransactionExecutor(txClient, ImmutableList.<TransactionAware>of());
  }

  /**
   * Prepares arguments which {@link SparkProgramWrapper} is submitted to {@link SparkSubmit} to run.
   *
   * @param sparkSpec     {@link SparkSpecification} of this job
   * @param conf          {@link Configuration} of the job whose {@link MRConfig#FRAMEWORK_NAME} specifies the mode in
   *                      which spark runs
   * @param programJar    {@link Location} copy of user program
   * @param dependencyJar {@link Location} jar containing the dependencies of this job
   * @return String[] of arguments with which {@link SparkProgramWrapper} will be submitted
   */
  private String[] prepareSparkSubmitArgs(SparkSpecification sparkSpec, Configuration conf,
                                          File programJar, File dependencyJar, File dummyJar, File hConfFile) {

    String archives = Joiner.on(',').join(programJar.getAbsolutePath(), dependencyJar.getAbsolutePath());

    return new String[]{"--class", SparkProgramWrapper.class.getName(), "--archives",
      archives, "--files", hConfFile.getAbsolutePath(), "--master", conf.get(MRConfig.FRAMEWORK_NAME),
      dummyJar.getAbsolutePath(), sparkSpec.getMainClassName()};
  }

  /**
   * Packages all the dependencies of the Spark job. It contains all CDAP classes that are needed to run the
   * user spark program.
   *
   * @param targetDir directory for the file to be created in
   * @return {@link Location} of the dependency jar
   * @throws IOException if failed to package the jar
   */
  private File buildDependencyJar(File targetDir) throws IOException {
    Location tempLocation = new LocalLocationFactory(targetDir).create("spark").getTempFile(".jar");

    ApplicationBundler appBundler = new ApplicationBundler(ImmutableList.of("org.apache.hadoop",
                                                                            "org.apache.spark",
                                                                            "scala"),
                                                           ImmutableList.of("org.apache.hadoop.hbase",
                                                                            "org.apache.hadoop.hive"));
    appBundler.createBundle(tempLocation, SparkProgramWrapper.class);
    return new File(tempLocation.toURI());
  }

  private File copyProgramJar(Location programJar, File targetDir) throws IOException {
    File tempFile = File.createTempFile("program", ".jar", targetDir);

    LOG.debug("Copy program jar from {} to {}", programJar, tempFile);
    Files.copy(Locations.newInputSupplier(programJar), tempFile);
    return tempFile;
  }

  /**
   * Generates an empty JAR file.
   *
   * @return The generated {@link File}.
   */
  private File generateDummyJar(File targetDir) throws IOException {
    File tempFile = File.createTempFile("dummy", ".jar", targetDir);
    JarOutputStream output = new JarOutputStream(new FileOutputStream(tempFile));
    output.close();
    return tempFile;
  }

  private File saveHConf(Configuration hConf, File targetDir) throws IOException {
    File file = File.createTempFile("hConf", ".xml", targetDir);
    try (Writer writer = Files.newWriter(file, Charsets.UTF_8)) {
      hConf.writeXml(writer);
    }
    return file;
  }

  private Runnable createCleanupTask(final File directory) {
    return new Runnable() {

      @Override
      public void run() {
        try {
          DirUtils.deleteDirectoryContents(directory);
        } catch (IOException e) {
          LOG.warn("Failed to cleanup directory {}", directory);
        }
      }
    };
  }
}
