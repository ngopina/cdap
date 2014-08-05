/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.explore.jdbc;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.FixedAddressExploreClient;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Explore JDBC Driver. A proper URL is of the form: jdbc:cdap://<host>:<port>?<param1>=<value1>[&<param2>=<value2>],
 * Where host and port point to CDAP http interface where Explore is enabled, and the additional parameters are from
 * the {@link co.cask.cdap.explore.jdbc.ExploreDriver.ConnectionParams.Info} enum.
 */
public class ExploreDriver implements Driver {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreDriver.class);
  static {
    try {
      java.sql.DriverManager.registerDriver(new ExploreDriver());
    } catch (SQLException e) {
      LOG.error("Caught exception when registering CDAP JDBC Driver", e);
    }
  }

  private static final Pattern CONNECTION_URL_PATTERN = Pattern.compile(Constants.Explore.Jdbc.URL_PREFIX + ".*");

  // The explore jdbc driver is not JDBC compliant, as tons of functionalities are missing
  private static final boolean JDBC_COMPLIANT = false;

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }

    ConnectionParams params = parseConnectionUrl(url);

    String authToken = null;

    List<String> tokenParams = Lists.newArrayList(params.getExtraInfos().get(ConnectionParams.Info.EXPLORE_AUTH_TOKEN));
    if (tokenParams != null && !tokenParams.isEmpty() && !tokenParams.get(0).isEmpty()) {
      authToken = tokenParams.get(0);
    }

    ExploreClient exploreClient = new FixedAddressExploreClient(params.getHost(), params.getPort(), authToken);
    if (!exploreClient.isServiceAvailable()) {
      throw new SQLException("Cannot connect to " + url + ", service unavailable");
    }
    return new ExploreConnection(exploreClient);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return CONNECTION_URL_PATTERN.matcher(url).matches();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Get the major version number of the Explore driver.
   */
  static int getMajorDriverVersion() {
    // TODO make it dynamic [REACTOR-319]
    return 2;
  }

  /**
   * Get the minor version number of the Explore driver.
   */
  static int getMinorDriverVersion() {
    // TODO make it dynamic [REACTOR-319]
    return 4;
  }

  @Override
  public int getMajorVersion() {
    return getMajorDriverVersion();
  }

  @Override
  public int getMinorVersion() {
    return getMinorDriverVersion();
  }

  @Override
  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }

  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Parse Explore connection url string to retrieve the necessary parameters to connect to CDAP.
   * Package visibility for testing.
   */
  ConnectionParams parseConnectionUrl(String url) {
    // URI does not accept two semicolons in a URL string, hence the substring
    URI jdbcURI = URI.create(url.substring(ExploreJDBCUtils.URI_JDBC_PREFIX.length()));
    String host = jdbcURI.getHost();
    int port = jdbcURI.getPort();
    ImmutableMultimap.Builder<ConnectionParams.Info, String>  builder = ImmutableMultimap.builder();

    try {
      // get the query params
      String query = jdbcURI.getQuery();
      if (query != null) {
        String decoded  = URLDecoder.decode(query, "UTF-8");
        Iterator<String> iterator = Splitter.on("&").split(decoded).iterator();
        while (iterator.hasNext()) {
          // Need to do it twice because of error in guava libs Issue: 1577
          String [] splits = (iterator.next()).split("=");
          // splits[0] is the key. Rest are values.
          if (splits.length > 0) {
            ConnectionParams.Info info = ConnectionParams.Info.fromStr(splits[0]);
            if (info != null) {
              for (int i = 1; i < splits.length; i++) {
                builder.putAll(info, splits[i].split(","));
              }
            }
          }
        }
      }
    } catch (UnsupportedEncodingException e) {
      throw Throwables.propagate(e);
    }
    return new ConnectionParams(host, port, builder.build());
  }

  /**
   * Explore connection parameters.
   */
  public static final class ConnectionParams {

    /**
     * Extra Explore connection parameter.
     */
    public enum Info {
      EXPLORE_AUTH_TOKEN("auth.token");

      private String name;

      private Info(String name) {
        this.name = name;
      }

      public String getName() {
        return name;
      }

      public static Info fromStr(String name) {
        for (Info info : Info.values()) {
          if (info.getName().equals(name)) {
            return info;
          }
        }
        return null;
      }
    }

    private final String host;
    private final int port;
    private final Multimap<Info, String> extraInfos;

    private ConnectionParams(String host, int port, Multimap<Info, String> extraInfos) {
      this.host = host;
      this.port = port;
      this.extraInfos = extraInfos;
    }

    public Multimap<Info, String> getExtraInfos() {
      return extraInfos;
    }

    public int getPort() {
      return port;
    }

    public String getHost() {
      return host;
    }
  }
}
