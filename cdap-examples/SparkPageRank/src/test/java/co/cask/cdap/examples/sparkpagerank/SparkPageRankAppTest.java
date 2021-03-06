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

package co.cask.cdap.examples.sparkpagerank;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class SparkPageRankAppTest extends TestBase {

  private static final String URL_1 = "http://example.com/page1";
  private static final String URL_2 = "http://example.com/page2";
  private static final String URL_3 = "http://example.com/page3";

  private static final String RANK = "14";

  @Test
  public void test() throws Exception {
    // Deploy the SparkPageRankApp
    ApplicationManager appManager = deployApplication(SparkPageRankApp.class);

    // Send a stream events to the Stream
    StreamManager streamManager = getStreamManager(SparkPageRankApp.BACKLINK_URL_STREAM);
    streamManager.send(Joiner.on(" ").join(URL_1, URL_2));
    streamManager.send(Joiner.on(" ").join(URL_1, URL_3));
    streamManager.send(Joiner.on(" ").join(URL_2, URL_1));
    streamManager.send(Joiner.on(" ").join(URL_3, URL_1));

    // Start GoogleTypePR
    ServiceManager transformServiceManager =
      appManager.getServiceManager(SparkPageRankApp.GOOGLE_TYPE_PR_SERVICE_NAME).start();
    // Start CentersService
    ServiceManager serviceManager = appManager.getServiceManager(SparkPageRankApp.RANKS_SERVICE_NAME).start();

    // Wait for GoogleTypePR service to start since the Spark program needs it
    transformServiceManager.waitForStatus(true);

    // Start the SparkPageRankProgram
    SparkManager sparkManager = appManager.getSparkManager("SparkPageRankProgram").start();
    sparkManager.waitForFinish(60, TimeUnit.SECONDS);

    // Wait for ranks service to start
    serviceManager.waitForStatus(true);

    //Query for rank
    URL ranksURL = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS),
                           SparkPageRankApp.RanksServiceHandler.RANKS_SERVICE_PATH);
    HttpURLConnection ranksURLConnection = (HttpURLConnection) ranksURL.openConnection();

    try {
      ranksURLConnection.setDoOutput(true);
      ranksURLConnection.setRequestMethod("POST");
      ranksURLConnection.getOutputStream().write(("{\"url\":\"" + URL_1 + "\"}").getBytes(Charsets.UTF_8));

      Assert.assertEquals(HttpURLConnection.HTTP_OK, ranksURLConnection.getResponseCode());

      if (ranksURLConnection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(ranksURLConnection.getInputStream()));
        Assert.assertEquals(RANK, reader.readLine());
      }
    } finally {
      ranksURLConnection.disconnect();
    }
  }

  private String requestService(URL url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    try {
      return new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }
}
