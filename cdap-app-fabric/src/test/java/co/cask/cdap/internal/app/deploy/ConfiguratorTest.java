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

package co.cask.cdap.internal.app.deploy;

import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.ConfigResponse;
import co.cask.cdap.app.deploy.Configurator;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.test.AppJarHelper;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.TimeUnit;

/**
 * Tests the configurators.
 *
 * NOTE: Till we can build the JAR it's difficult to test other configurators
 * {@link co.cask.cdap.internal.app.deploy.InMemoryConfigurator} &
 * {@link co.cask.cdap.internal.app.deploy.SandboxConfigurator}
 */
public class ConfiguratorTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testInMemoryConfigurator() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, WordCountApp.class);

    // Create a configurator that is testable. Provide it a application.
    Configurator configurator = new InMemoryConfigurator(appJar, "");

    // Extract response from the configurator.
    ListenableFuture<ConfigResponse> result = configurator.config();
    ConfigResponse response = result.get(10, TimeUnit.SECONDS);
    Assert.assertNotNull(response);

    // Deserialize the JSON spec back into Application object.
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification specification = adapter.fromJson(response.get());
    Assert.assertNotNull(specification);
    Assert.assertTrue(specification.getName().equals("WordCountApp")); // Simple checks.
    Assert.assertTrue(specification.getFlows().size() == 1); // # of flows.
  }

  @Test
  public void testAppWithConfig() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TMP_FOLDER.newFolder());
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, ConfigTestApp.class);

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("myStream", "myTable");
    Configurator configuratorWithConfig = new InMemoryConfigurator(appJar, new Gson().toJson(config));

    ListenableFuture<ConfigResponse> result = configuratorWithConfig.config();
    ConfigResponse response = result.get(10, TimeUnit.SECONDS);
    Assert.assertNotNull(response);

    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification specification = adapter.fromJson(response.get());
    Assert.assertNotNull(specification);
    Assert.assertTrue(specification.getStreams().size() == 1);
    Assert.assertTrue(specification.getStreams().containsKey("myStream"));
    Assert.assertTrue(specification.getDatasets().size() == 1);
    Assert.assertTrue(specification.getDatasets().containsKey("myTable"));

    Configurator configuratorWithoutConfig = new InMemoryConfigurator(appJar, null);
    result = configuratorWithoutConfig.config();
    response = result.get(10, TimeUnit.SECONDS);
    Assert.assertNotNull(response);

    specification = adapter.fromJson(response.get());
    Assert.assertNotNull(specification);
    Assert.assertTrue(specification.getStreams().size() == 1);
    Assert.assertTrue(specification.getStreams().containsKey(ConfigTestApp.DEFAULT_STREAM));
    Assert.assertTrue(specification.getDatasets().size() == 1);
    Assert.assertTrue(specification.getDatasets().containsKey(ConfigTestApp.DEFAULT_TABLE));
  }

}
