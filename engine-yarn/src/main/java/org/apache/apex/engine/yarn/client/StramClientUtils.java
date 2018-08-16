/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.yarn.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.api.Settings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import com.datatorrent.stram.StramClient;
import com.datatorrent.stram.util.ConfigUtils;

public class StramClientUtils extends com.datatorrent.stram.client.StramClientUtils
{
  private static final Logger LOG = LoggerFactory.getLogger(StramClientUtils.class);

  public static class YarnClientHelper
  {
    private static final Logger LOG = LoggerFactory.getLogger(YarnClientHelper.class);
    // Configuration
    private final Configuration conf;
    // RPC to communicate to RM
    private final YarnRPC rpc;

    public YarnClientHelper(Configuration conf)
    {
      // Set up the configuration and RPC
      this.conf = conf;
      this.rpc = YarnRPC.create(conf);
    }

    public Configuration getConf()
    {
      return this.conf;
    }

//    public YarnRPC getYarnRPC()
//    {
//      return rpc;
//    }

//    /**
//     * Connect to the Resource Manager/Applications Manager<p>
//     *
//     * @return Handle to communicate with the ASM
//     * @throws IOException
//     */
//    public ApplicationClientProtocol connectToASM() throws IOException
//    {
//      YarnConfiguration yarnConf = new YarnConfiguration(conf);
//      InetSocketAddress rmAddress = yarnConf.getSocketAddr(
//          YarnConfiguration.RM_ADDRESS,
//          YarnConfiguration.DEFAULT_RM_ADDRESS,
//          YarnConfiguration.DEFAULT_RM_PORT);
//      LOG.debug("Connecting to ResourceManager at " + rmAddress);
//      return ((ApplicationClientProtocol)rpc.getProxy(ApplicationClientProtocol.class, rmAddress, conf));
//    }

    /**
     * Connect to the Resource Manager<p>
     *
     * @return Handle to communicate with the RM
     */
    public ApplicationMasterProtocol connectToRM()
    {
      InetSocketAddress rmAddress = conf.getSocketAddr(
          YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
      LOG.debug("Connecting to ResourceManager at " + rmAddress);
      return ((ApplicationMasterProtocol)rpc.getProxy(ApplicationMasterProtocol.class, rmAddress, conf));
    }

  }

  /**
   * Bunch of utilities that ease repeating interactions with {@link ClientRMProxy}<p>
   */
  public static class ClientRMHelper
  {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRMHelper.class);

    private static final String RM_HOSTNAME_PREFIX = YarnConfiguration.RM_PREFIX + "hostname.";

    private final YarnClient clientRM;
    private final Configuration conf;

    public ClientRMHelper(YarnClient yarnClient, Configuration conf) throws IOException
    {
      this.clientRM = yarnClient;
      this.conf = conf;
    }

    public interface AppStatusCallback
    {
      boolean exitLoop(ApplicationReport report);

    }

    /**
     * Monitor the submitted application for completion. Kill application if time expires.
     *
     * @param appId         Application Id of application to be monitored
     * @param callback
     * @param timeoutMillis
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    @SuppressWarnings("SleepWhileInLoop")
    public boolean waitForCompletion(ApplicationId appId, AppStatusCallback callback, long timeoutMillis) throws YarnException, IOException
    {
      long startMillis = System.currentTimeMillis();
      while (true) {

        // Check app status every 1 second.
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.debug("Thread sleep in monitoring loop interrupted");
        }

        ApplicationReport report = clientRM.getApplicationReport(appId);
        if (callback.exitLoop(report) == true) {
          return true;
        }

        YarnApplicationState state = report.getYarnApplicationState();
        FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
        if (YarnApplicationState.FINISHED == state) {
          if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
            LOG.info("Application has completed successfully. Breaking monitoring loop");
            return true;
          } else {
            LOG.info("Application finished unsuccessfully."
                + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                + ". Breaking monitoring loop");
            return false;
          }
        } else if (YarnApplicationState.KILLED == state
            || YarnApplicationState.FAILED == state) {
          LOG.info("Application did not finish."
              + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
              + ". Breaking monitoring loop");
          return false;
        }

        if (System.currentTimeMillis() - startMillis > timeoutMillis) {
          LOG.info("Reached specified timeout. Killing application");
          clientRM.killApplication(appId);
          return false;
        }
      }
    }

    // TODO: HADOOP UPGRADE - replace with YarnConfiguration constants
    private Token<RMDelegationTokenIdentifier> getRMHAToken(org.apache.hadoop.yarn.api.records.Token rmDelegationToken)
    {
      // Build a list of service addresses to form the service name
      ArrayList<String> services = new ArrayList<>();
      for (String rmId : ConfigUtils.getRMHAIds(conf)) {
        LOG.info("Yarn Resource Manager id: {}", rmId);
        // Set RM_ID to get the corresponding RM_ADDRESS
        services.add(SecurityUtil.buildTokenService(getRMHAAddress(rmId)).toString());
      }
      Text rmTokenService = new Text(Joiner.on(',').join(services));

      return new Token<>(
          rmDelegationToken.getIdentifier().array(),
          rmDelegationToken.getPassword().array(),
          new Text(rmDelegationToken.getKind()),
          rmTokenService);
    }

    public void addRMDelegationToken(final String renewer, final Credentials credentials) throws IOException, YarnException
    {
      // Get the ResourceManager delegation rmToken
      final org.apache.hadoop.yarn.api.records.Token rmDelegationToken = clientRM.getRMDelegationToken(new Text(renewer));

      Token<RMDelegationTokenIdentifier> token;
      // TODO: Use the utility method getRMDelegationTokenService in ClientRMProxy to remove the separate handling of
      // TODO: HA and non-HA cases when hadoop dependency is changed to hadoop 2.4 or above
      if (ConfigUtils.isRMHAEnabled(conf)) {
        LOG.info("Yarn Resource Manager HA is enabled");
        token = getRMHAToken(rmDelegationToken);
      } else {
        LOG.info("Yarn Resource Manager HA is not enabled");
        InetSocketAddress rmAddress = conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_PORT);

        token = ConverterUtils.convertFromYarn(rmDelegationToken, rmAddress);
      }

      LOG.info("RM dt {}", token);

      credentials.addToken(token.getService(), token);
    }

    public InetSocketAddress getRMHAAddress(String rmId)
    {
      YarnConfiguration yarnConf = getYarnConfiguration(conf);
      yarnConf.set(ConfigUtils.RM_HA_ID, rmId);
      InetSocketAddress socketAddr = yarnConf.getSocketAddr(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
      yarnConf.unset(ConfigUtils.RM_HA_ID);
      return socketAddr;
    }

  }

  public static YarnClient createYarnClient(Configuration conf)
  {
    YarnClient client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();
    return client;
  }


  public static ApplicationReport getStartedAppInstanceByName(YarnClient clientRMService, String appName, String user, String excludeAppId) throws YarnException, IOException
  {
    List<ApplicationReport> applications = clientRMService.getApplications(Sets.newHashSet(StramClient.APPLICATION_TYPE, StramClient.APPLICATION_TYPE_DEPRECATED), EnumSet.of(YarnApplicationState.RUNNING,
        YarnApplicationState.ACCEPTED,
        YarnApplicationState.NEW,
        YarnApplicationState.NEW_SAVING,
        YarnApplicationState.SUBMITTED));
    // see whether there is an app with the app name and user name running
    for (ApplicationReport app : applications) {
      if (!app.getApplicationId().toString().equals(excludeAppId)
          && app.getName().equals(appName)
          && app.getUser().equals(user)) {
        return app;
      }
    }
    return null;
  }

  public static List<ApplicationReport> cleanAppDirectories(YarnClient clientRMService, Configuration conf, FileSystem fs, long finishedBefore)
      throws IOException, YarnException
  {
    List<ApplicationReport> result = new ArrayList<>();
    List<ApplicationReport> applications = clientRMService.getApplications(Sets.newHashSet(StramClient.APPLICATION_TYPE, StramClient.APPLICATION_TYPE_DEPRECATED),
        EnumSet.of(YarnApplicationState.FAILED, YarnApplicationState.FINISHED, YarnApplicationState.KILLED));
    Path appsBasePath = new Path(com.datatorrent.stram.client.StramClientUtils.getApexDFSRootDir(fs, conf), com.datatorrent.stram.client.StramClientUtils.SUBDIR_APPS);
    for (ApplicationReport ar : applications) {
      long finishTime = ar.getFinishTime();
      if (finishTime < finishedBefore) {
        try {
          Path appPath = new Path(appsBasePath, ar.getApplicationId().toString());
          if (fs.isDirectory(appPath)) {
            LOG.debug("Deleting finished application data for {}", ar.getApplicationId());
            fs.delete(appPath, true);
            result.add(ar);
          }
        } catch (Exception ex) {
          LOG.warn("Cannot delete application data for {}", ar.getApplicationId(), ex);
          continue;
        }
      }
    }
    return result;
  }

  /**
   * Return a YarnConfiguration instance from a Configuration instance
   * @param conf The configuration instance
   * @return The YarnConfiguration instance
   */
  public static YarnConfiguration getYarnConfiguration(Configuration conf)
  {
    YarnConfiguration yarnConf;
    if (conf instanceof YarnConfiguration) {
      yarnConf = (YarnConfiguration)conf;
    } else {
      yarnConf = new YarnConfiguration(conf);
    }
    return yarnConf;
  }

  public static InetSocketAddress getRMWebAddress(Configuration conf, String rmId)
  {
    boolean sslEnabled = conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY, CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_DEFAULT);
    return getRMWebAddress(conf, sslEnabled, rmId);
  }

  /**
   * Get the RM webapp address. The configuration that is passed in should not be used by other threads while this
   * method is executing.
   * @param conf The configuration
   * @param sslEnabled Whether SSL is enabled or not
   * @param rmId If HA is enabled the resource manager id
   * @return The webapp socket address
   */
  public static InetSocketAddress getRMWebAddress(Configuration conf, boolean sslEnabled, String rmId)
  {
    boolean isHA = (rmId != null);
    if (isHA) {
      conf = getYarnConfiguration(conf);
      conf.set(ConfigUtils.RM_HA_ID, rmId);
    }
    InetSocketAddress address;
    if (sslEnabled) {
      address = conf.getSocketAddr(Settings.Strings.RM_WEBAPP_HTTPS_ADDRESS.getValue(),
          Settings.Strings.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS.getValue(),
          Settings.Integers.DEFAULT_RM_WEBAPP_HTTPS_PORT.getValue());
    } else {
      address = conf.getSocketAddr(Settings.Strings.RM_WEBAPP_ADDRESS.getValue(),
          Settings.Strings.DEFAULT_RM_WEBAPP_ADDRESS.getValue(),
          Settings.Integers.DEFAULT_RM_WEBAPP_PORT.getValue());
    }
    if (isHA) {
      conf.unset(ConfigUtils.RM_HA_ID);
    }
    LOG.info("rm webapp address setting {}", address);
    LOG.debug("rm setting sources {}", conf.getPropertySources(Settings.Strings.RM_WEBAPP_ADDRESS.getValue()));
    InetSocketAddress resolvedSocketAddress = NetUtils.getConnectAddress(address);
    InetAddress resolved = resolvedSocketAddress.getAddress();
    if (resolved == null || resolved.isAnyLocalAddress() || resolved.isLoopbackAddress()) {
      try {
        resolvedSocketAddress = InetSocketAddress.createUnresolved(InetAddress.getLocalHost().getCanonicalHostName(), address.getPort());
      } catch (UnknownHostException e) {
        //Ignore and fallback.
      }
    }
    return resolvedSocketAddress;
  }

  public static List<InetSocketAddress> getRMAddresses(Configuration conf)
  {

    List<InetSocketAddress> rmAddresses = new ArrayList<>();
    if (ConfigUtils.isRMHAEnabled(conf)) {
      // HA is enabled get all
      for (String rmId : ConfigUtils.getRMHAIds(conf)) {
        InetSocketAddress socketAddress = getRMWebAddress(conf, rmId);
        rmAddresses.add(socketAddress);
      }
    } else {
      InetSocketAddress socketAddress = getRMWebAddress(conf, null);
      rmAddresses.add(socketAddress);
    }
    return rmAddresses;
  }

}
