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
package org.apache.apex.engine.yarn;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.common.util.PropertiesHelper;
import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.apex.engine.api.StreamingAppMaster;
import org.apache.apex.engine.api.StreamingAppMasterContext;
import org.apache.apex.engine.api.plugin.DAGExecutionPlugin;
import org.apache.apex.engine.api.plugin.PluginLocator;
import org.apache.apex.engine.events.grouping.GroupingManager;
import org.apache.apex.engine.events.grouping.GroupingRequest.EventGroupId;
import org.apache.apex.engine.plugin.ApexPluginDispatcher;
import org.apache.apex.engine.plugin.DefaultApexPluginDispatcher;
import org.apache.apex.engine.plugin.loaders.ChainedPluginLocator;
import org.apache.apex.engine.plugin.loaders.PropertyBasedPluginLocator;
import org.apache.apex.engine.plugin.loaders.ServiceLoaderBasedPluginLocator;
import org.apache.apex.engine.yarn.client.StramClientUtils;
import org.apache.apex.engine.yarn.security.TokenRenewer;
import org.apache.apex.engine.yarn.webapp.StramWebApp;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StringCodec;
import com.datatorrent.stram.FSRecoveryHandler;
import com.datatorrent.stram.IApexContainerLaunchContext;
import com.datatorrent.stram.LaunchContainerRunnable;
import com.datatorrent.stram.RecoverableRpcProxy;
import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerAgent;
import com.datatorrent.stram.StreamingContainerAgent.ContainerStartRequest;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.StringCodecs;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.appdata.AppDataPushAgent;
import com.datatorrent.stram.engine.StreamingContainer;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.security.StramDelegationTokenIdentifier;
import com.datatorrent.stram.security.StramDelegationTokenManager;
import com.datatorrent.stram.util.IAllocatedContainer;
import com.datatorrent.stram.util.IApplicationAttemptId;
import com.datatorrent.stram.util.MasterServiceUtil;
import com.datatorrent.stram.util.MasterServiceUtil.NodeFailureStats;
import com.datatorrent.stram.util.SecurityUtils;

import static java.lang.Thread.sleep;

/**
 * Streaming Application Master
 *
 * @since 0.3.2
 */
public class StreamingAppMasterService extends CompositeService implements StreamingAppMaster
{
  private static final Logger LOG = LoggerFactory.getLogger(StreamingAppMasterService.class);
  private static final long DELEGATION_KEY_UPDATE_INTERVAL = 24 * 60 * 60 * 1000;
  private static final long DELEGATION_TOKEN_MAX_LIFETIME = Long.MAX_VALUE / 2;
  private static final long DELEGATION_TOKEN_RENEW_INTERVAL = Long.MAX_VALUE / 2;
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL = 24 * 60 * 60 * 1000;
  private static final int UPDATE_NODE_REPORTS_INTERVAL = 10 * 60 * 1000;
  private AMRMClient<ContainerRequest> amRmClient;
  private NMClientAsync nmClient;
  private LogicalPlan dag;
  // Application Attempt Id ( combination of attemptId and fail count )
  private IApplicationAttemptId appAttemptID;
  // Hostname of the container
  private final String appMasterHostname = "";
  // Simple flag to denote whether all works is done
  private boolean appDone = false;
  // Counter for completed containers ( complete denotes successful or failed )
  private final AtomicInteger numCompletedContainers = new AtomicInteger();
  // Set of nodes marked blacklisted due to consecutive container failures on the nodes
  private final Set<String> failedBlackListedNodes = Sets.newHashSet();
  // Maintains max consecutive failures stats for nodes for blacklisting failing nodes
  private final Map<String, MasterServiceUtil.NodeFailureStats> failedContainerNodesMap = Maps.newHashMap();
  private final ConcurrentLinkedQueue<Runnable> pendingTasks = new ConcurrentLinkedQueue<>();
  // child container callback
  private StreamingContainerParent heartbeatListener;
  private com.datatorrent.stram.StreamingContainerManager dnmgr;
  private MasterServiceUtil.ClusterAppContextImpl appContext;
  private final Clock clock = new SystemClock();
  private final long startTime = clock.getTime();
  private final MasterServiceUtil.ClusterAppStats stats = new MasterServiceUtil.ClusterAppStats();
  private StramDelegationTokenManager delegationTokenManager = null;
  private AppDataPushAgent appDataPushAgent;
  private ApexPluginDispatcher apexPluginDispatcher;
  private final GroupingManager groupingManager = GroupingManager.getGroupingManagerInstance();
  private static final long REMOVE_CONTAINER_TIMEOUT = PropertiesHelper.getLong("org.apache.apex.nodemanager.containerKill.timeout", 30 * 1000, 0, Long.MAX_VALUE);
  private TokenRenewer tokenRenewer;

  public StreamingAppMasterService()
  {
    super(StreamingAppMasterService.class.getName());
  }

  @Override
  public void setup(StreamingAppMasterContext context)
  {
    CommandLine cliParser = context.getCommandLine();
    appAttemptID = new ApplicationAttemptId(cliParser);
    Configuration conf = ClusterProviderFactory.getProvider().getConfiguration().getNativeConfig();
    init(conf);
    start();
  }

  @Override
  public void teardown()
  {
    stop();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    LOG.info("Application master" + ", appId=" + appAttemptID.getApplicationId() + ", clustertimestamp=" + appAttemptID.getClusterTimestamp() + ", attemptId=" + appAttemptID.getApplicationAttemptId());

    FileInputStream fis = new FileInputStream("./" + LogicalPlan.SER_FILE_NAME);
    try {
      this.dag = LogicalPlan.read(fis);
    } finally {
      fis.close();
    }
    // "debug" simply dumps all data using LOG.info
    if (dag.isDebug()) {
      MasterServiceUtil.dumpOutDebugInfo(getConfig());
    }
    dag.setAttribute(LogicalPlan.APPLICATION_ATTEMPT_ID, appAttemptID.getApplicationAttemptId());
    FSRecoveryHandler recoveryHandler = new FSRecoveryHandler(dag.assertAppPath(), conf);
    this.dnmgr = StreamingContainerManager.getInstance(recoveryHandler, dag, true);
    stats.setDnmgr(dnmgr);
    dag = this.dnmgr.getLogicalPlan();
    this.appContext = new MasterServiceUtil.ClusterAppContextImpl(dag.getAttributes(), startTime, stats, appAttemptID);

    Map<Class<?>, Class<? extends StringCodec<?>>> codecs = dag.getAttributes().get(DAG.STRING_CODECS);
    StringCodecs.loadConverters(codecs);

    LOG.info("Starting application with {} operators in {} containers", dnmgr.getPhysicalPlan().getAllOperators().size(), dnmgr.getPhysicalPlan().getContainers().size());

    // Setup security configuration such as that for web security
    SecurityUtils.init(conf, dag.getValue(LogicalPlan.STRAM_HTTP_AUTHENTICATION));

    if (UserGroupInformation.isSecurityEnabled()) {
      // TODO :- Need to perform token renewal
      delegationTokenManager = new StramDelegationTokenManager(DELEGATION_KEY_UPDATE_INTERVAL, DELEGATION_TOKEN_MAX_LIFETIME, DELEGATION_TOKEN_RENEW_INTERVAL, DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL);
    }
    this.nmClient = new NMClientAsyncImpl(new NMCallbackHandler());
    addService(nmClient);
    this.amRmClient = AMRMClient.createAMRMClient();
    addService(amRmClient);

    // start RPC server
    int rpcListenerCount = dag.getValue(DAGContext.HEARTBEAT_LISTENER_THREAD_COUNT);
    this.heartbeatListener = new StreamingContainerParent(this.getClass().getName(), dnmgr, delegationTokenManager, rpcListenerCount);
    addService(heartbeatListener);

    AutoMetric.Transport appDataPushTransport = dag.getValue(LogicalPlan.METRICS_TRANSPORT);
    if (appDataPushTransport != null) {
      this.appDataPushAgent = new AppDataPushAgent(dnmgr, appContext);
      addService(this.appDataPushAgent);
    }
    initApexPluginDispatcher();

    // Initialize all services added above
    super.serviceInit(conf);
  }

  private void initApexPluginDispatcher()
  {
    PluginLocator<DAGExecutionPlugin> locator = new ChainedPluginLocator<>(new ServiceLoaderBasedPluginLocator<>(DAGExecutionPlugin.class),
        new PropertyBasedPluginLocator<>(DAGExecutionPlugin.class, PLUGINS_CONF_KEY));
    apexPluginDispatcher = new DefaultApexPluginDispatcher(locator, appContext, dnmgr, stats);
    dnmgr.apexPluginDispatcher = apexPluginDispatcher;
    addService(apexPluginDispatcher);
    apexPluginDispatcher.dispatch(new ApexPluginDispatcher.DAGChangeEvent(dnmgr.getLogicalPlan()));
  }

  @Override
  protected void serviceStart() throws Exception
  {
    super.serviceStart();
    if (UserGroupInformation.isSecurityEnabled()) {
      delegationTokenManager.startThreads();
    }

    // write the connect address for containers to DFS
    InetSocketAddress connectAddress = NetUtils.getConnectAddress(this.heartbeatListener.getAddress());
    URI connectUri = RecoverableRpcProxy.toConnectURI(connectAddress);
    FSRecoveryHandler recoveryHandler = new FSRecoveryHandler(dag.assertAppPath(), getConfig());
    recoveryHandler.writeConnectUri(connectUri.toString());

    // start web service
    try {
      org.mortbay.log.Log.setLog(null);
    } catch (Throwable throwable) {
      // SPOI-2687. As part of Pivotal Certification, we need to catch ClassNotFoundException as Pivotal was using
      // Jetty 7 where as other distros are using Jetty 6.
      // LOG.error("can't set the log to null: ", throwable);
    }

    try {
      Configuration config = org.apache.apex.engine.yarn.util.SecurityUtils.configureWebAppSecurity(getConfig(), dag.getValue(DAGContext.SSL_CONFIG));
      WebApp webApp = WebApps.$for("stram", StramAppContext.class, appContext, "ws").with(config).start(new StramWebApp(this.dnmgr));
      LOG.info("Started web service at port: " + webApp.port());
      // best effort to produce FQDN for the client to connect with
      // (when SSL is enabled, it may be required to match the certificate)
      connectAddress = NetUtils.getConnectAddress(webApp.getListenerAddress());
      String hostname = connectAddress.getAddress().getCanonicalHostName();
      if (hostname.equals(connectAddress.getAddress().getHostAddress())) {
        // lookup didn't yield a name
        hostname = connectAddress.getHostName();
      }

      appContext.setAppMasterTrackingUrl(hostname, webApp.port(), config);
    } catch (Exception e) {
      LOG.error("Webapps failed to start. Ignoring for now:", e);
    }
  }

  @Override
  protected void serviceStop() throws Exception
  {
    super.serviceStop();
    if (UserGroupInformation.isSecurityEnabled()) {
      delegationTokenManager.stopThreads();
    }
    if (nmClient != null) {
      nmClient.stop();
    }
    if (amRmClient != null) {
      amRmClient.stop();
    }
    if (dnmgr != null) {
      dnmgr.teardown();
    }
  }

  public boolean run() throws Exception
  {
    boolean status = true;
    try {
      StreamingContainer.eventloop.start();
      execute();
    } finally {
      StreamingContainer.eventloop.stop();
    }
    return status;
  }

  /**
   * Main run function for the application master
   *
   * @throws YarnException
   */
  @SuppressWarnings("SleepWhileInLoop")
  private void execute() throws YarnException, IOException
  {
    LOG.info("Starting ApplicationMaster");
    final Configuration conf = getConfig();
    if (UserGroupInformation.isSecurityEnabled()) {
      tokenRenewer = new TokenRenewer(dag, true, conf, Integer.toString(appAttemptID.getApplicationId()));
    }

    // Register self with ResourceManager
    RegisterApplicationMasterResponse response = amRmClient.registerApplicationMaster(appMasterHostname, 0, appContext.getAppMasterTrackingUrl());

    // Dump out information about cluster capability as seen by the resource manager
    int maxMem = response.getMaximumResourceCapability().getMemory();
    int maxVcores = response.getMaximumResourceCapability().getVirtualCores();
    int minMem = conf.getInt("yarn.scheduler.minimum-allocation-mb", 0);
    int minVcores = conf.getInt("yarn.scheduler.minimum-allocation-vcores", 0);
    LOG.info("Max mem {}m, Min mem {}m, Max vcores {} and Min vcores {} capabililty of resources in this cluster ", maxMem, minMem, maxVcores, minVcores);

    long blacklistRemovalTime = dag.getValue(DAGContext.BLACKLISTED_NODE_REMOVAL_TIME_MILLIS);
    int maxConsecutiveContainerFailures = dag.getValue(DAGContext.MAX_CONSECUTIVE_CONTAINER_FAILURES_FOR_BLACKLIST);
    LOG.info("Blacklist removal time in millis = {}, max consecutive node failure count = {}", blacklistRemovalTime, maxConsecutiveContainerFailures);
    // for locality relaxation fall back
    Map<ContainerStartRequest, MutablePair<Integer, ContainerRequest>> requestedResources = Maps.newHashMap();

    // Setup heartbeat emitter
    // TODO poll RM every now and then with an empty request to let RM know that we are alive
    // The heartbeat interval after which an AM is timed out by the RM is defined by a config setting:
    // RM_AM_EXPIRY_INTERVAL_MS with default defined by DEFAULT_RM_AM_EXPIRY_INTERVAL_MS
    // The allocate calls to the RM count as heartbeat so, for now, this additional heartbeat emitter
    // is not required.

    int loopCounter = -1;
    long nodeReportUpdateTime = 0;

    // keep track of already requested containers to not request them again while waiting for allocation
    int numRequestedContainers = 0;
    int numReleasedContainers = 0;
    int nextRequestPriority = 0;
    // Use override for resource requestor in case of cloudera distribution, to handle host specific requests
    ResourceRequestHandler resourceRequestor = System.getenv().containsKey("CDH_HADOOP_BIN") ? new BlacklistBasedResourceRequestHandler() : new ResourceRequestHandler();

    List<ContainerStartRequest> pendingContainerStartRequests = new LinkedList<>();
    try (YarnClient clientRMService = StramClientUtils.createYarnClient(conf)) {

      try {
        // YARN-435
        // we need getClusterNodes to populate the initial node list,
        // subsequent updates come through the heartbeat response

        ApplicationReport ar = StramClientUtils.getStartedAppInstanceByName(clientRMService, dag.getAttributes().get(DAG.APPLICATION_NAME), UserGroupInformation.getLoginUser().getUserName(), dag.getAttributes().get(DAG.APPLICATION_ID));
        if (ar != null) {
          appDone = true;
          dnmgr.setShutdownDiagnosticsMessage(String.format("Application master failed due to application %s with duplicate application name \"%s\" by the same user \"%s\" is already started.",
              ar.getApplicationId().toString(), ar.getName(), ar.getUser()));
          LOG.info("Forced shutdown due to {}", dnmgr.getShutdownDiagnosticsMessage());
          finishApplication(FinalApplicationStatus.FAILED);
          return;
        }
        resourceRequestor.updateNodeReports(clientRMService.getNodeReports());
        nodeReportUpdateTime = System.currentTimeMillis() + UPDATE_NODE_REPORTS_INTERVAL;
      } catch (Exception e) {
        throw new RuntimeException("Failed to retrieve cluster nodes report.", e);
      }

      List<Container> containers = response.getContainersFromPreviousAttempts();

      // Running containers might take a while to register with the new app master and send the heartbeat signal.
      int waitForRecovery = containers.size() > 0 ? dag.getValue(LogicalPlan.HEARTBEAT_TIMEOUT_MILLIS) / 1000 : 0;

      List<ContainerId> releasedContainers = previouslyAllocatedContainers(containers);
      FinalApplicationStatus finalStatus = FinalApplicationStatus.SUCCEEDED;
      final InetSocketAddress rmAddress = conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_PORT);

      while (!appDone) {
        loopCounter++;
        final long currentTimeMillis = System.currentTimeMillis();

        if (tokenRenewer != null) {
          tokenRenewer.checkAndRenew();
        }

        if (currentTimeMillis > nodeReportUpdateTime) {
          resourceRequestor.updateNodeReports(clientRMService.getNodeReports());
          nodeReportUpdateTime = currentTimeMillis + UPDATE_NODE_REPORTS_INTERVAL;
        }

        Runnable r;
        while ((r = this.pendingTasks.poll()) != null) {
          r.run();
        }

        // log current state
        /*
         * LOG.info("Current application state: loop=" + loopCounter + ", appDone=" + appDone + ", total=" +
         * numTotalContainers + ", requested=" + numRequestedContainers + ", completed=" + numCompletedContainers +
         * ", failed=" + numFailedContainers + ", currentAllocated=" + this.allAllocatedContainers.size());
         */
        // Sleep before each loop when asking RM for containers
        // to avoid flooding RM with spurious requests when it
        // need not have any available containers
        try {
          sleep(1000);
        } catch (InterruptedException e) {
          LOG.info("Sleep interrupted", e);
        }

        // Setup request to be sent to RM to allocate containers
        List<ContainerRequest> containerRequests = new ArrayList<>();
        List<ContainerRequest> removedContainerRequests = new ArrayList<>();

        // request containers for pending deploy requests
        if (!dnmgr.getContainerStartRequests().isEmpty()) {
          ContainerStartRequest csr;
          while ((csr = dnmgr.getContainerStartRequests().poll()) != null) {
            PTContainer container = csr.getContainer();
            if (container.getRequiredMemoryMB() > maxMem) {
              LOG.warn("Container memory {}m above max threshold of cluster. Using max value {}m.", container.getRequiredMemoryMB(), maxMem);
              container.setRequiredMemoryMB(maxMem);
            }
            if (container.getRequiredMemoryMB() < minMem) {
              container.setRequiredMemoryMB(minMem);
            }
            if (container.getRequiredVCores() > maxVcores) {
              LOG.warn("Container vcores {} above max threshold of cluster. Using max value {}.", container.getRequiredVCores(), maxVcores);
              container.setRequiredVCores(maxVcores);
            }
            if (container.getRequiredVCores() < minVcores) {
              container.setRequiredVCores(minVcores);
            }
            container.setResourceRequestPriority(nextRequestPriority++);
            ContainerRequest cr = resourceRequestor.createContainerRequest(csr, true);
            if (cr == null) {
              pendingContainerStartRequests.add(csr);
            } else {
              resourceRequestor.addContainerRequest(requestedResources, loopCounter, containerRequests, csr, cr);
            }
          }
        }

        // If all other requests are allocated, retry pending requests which need host availability
        if (containerRequests.isEmpty() && !pendingContainerStartRequests.isEmpty()) {
          List<ContainerStartRequest> removalList = new LinkedList<>();
          for (ContainerStartRequest csr : pendingContainerStartRequests) {
            ContainerRequest cr = resourceRequestor.createContainerRequest(csr, true);
            if (cr != null) {
              resourceRequestor.addContainerRequest(requestedResources, loopCounter, containerRequests, csr, cr);
              removalList.add(csr);
            }
          }
          pendingContainerStartRequests.removeAll(removalList);
        }

        resourceRequestor.reissueContainerRequests(amRmClient, requestedResources, loopCounter, resourceRequestor, containerRequests, removedContainerRequests);

      /* Remove nodes from blacklist after timeout */
        List<String> blacklistRemovals = new ArrayList<>();
        for (String hostname : failedBlackListedNodes) {
          Long timeDiff = currentTimeMillis - failedContainerNodesMap.get(hostname).getBlackListAdditionTime();
          if (timeDiff >= blacklistRemovalTime) {
            blacklistRemovals.add(hostname);
            failedContainerNodesMap.remove(hostname);
          }
        }
        if (!blacklistRemovals.isEmpty()) {
          amRmClient.updateBlacklist(null, blacklistRemovals);
          LOG.info("Removing nodes {} from blacklist: time elapsed since last blacklisting due to failure is greater than specified timeout", blacklistRemovals.toString());
          failedBlackListedNodes.removeAll(blacklistRemovals);
        }

        numRequestedContainers += containerRequests.size() - removedContainerRequests.size();
        AllocateResponse amResp = sendContainerAskToRM(containerRequests, removedContainerRequests, releasedContainers);
        if (amResp.getAMCommand() != null) {
          LOG.info(" statement executed:{}", amResp.getAMCommand());
          switch (amResp.getAMCommand()) {
            case AM_RESYNC:
            case AM_SHUTDOWN:
              throw new YarnRuntimeException("Received the " + amResp.getAMCommand() + " command from RM");
            default:
              throw new YarnRuntimeException("Received the " + amResp.getAMCommand() + " command from RM");

          }
        }
        releasedContainers.clear();

        // Retrieve list of allocated containers from the response
        List<Container> newAllocatedContainers = amResp.getAllocatedContainers();
        // LOG.info("Got response from RM for container ask, allocatedCnt=" + newAllocatedContainers.size());
        numRequestedContainers -= newAllocatedContainers.size();
        long timestamp = System.currentTimeMillis();
        for (Container allocatedContainer : newAllocatedContainers) {

          LOG.info("Got new container." + ", containerId=" + allocatedContainer.getId() + ", containerNode=" + allocatedContainer.getNodeId() + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory" + allocatedContainer.getResource().getMemory() + ", priority" + allocatedContainer.getPriority());
          // + ", containerToken" + allocatedContainer.getContainerToken().getIdentifier().toString());

          boolean alreadyAllocated = true;
          ContainerStartRequest csr = null;
          for (Map.Entry<ContainerStartRequest, MutablePair<Integer, ContainerRequest>> entry : requestedResources.entrySet()) {
            if (entry.getKey().getContainer().getResourceRequestPriority() == allocatedContainer.getPriority().getPriority()) {
              alreadyAllocated = false;
              csr = entry.getKey();
              break;
            }
          }

          if (alreadyAllocated) {
            LOG.info("Releasing {} as resource with priority {} was already assigned", allocatedContainer.getId(), allocatedContainer.getPriority());
            releasedContainers.add(allocatedContainer.getId());
            numReleasedContainers++;
            numRequestedContainers++;    // undo the decrement above for this allocated container
            continue;
          }
          if (csr != null) {
            requestedResources.remove(csr);
          }

          // allocate resource to container
          ContainerResource resource = new ContainerResource(allocatedContainer.getPriority().getPriority(), allocatedContainer.getId().toString(), allocatedContainer.getNodeId().toString(), allocatedContainer.getResource().getMemory(), allocatedContainer.getResource().getVirtualCores(), allocatedContainer.getNodeHttpAddress());
          StreamingContainerAgent sca = dnmgr.assignContainer(resource, null);

          if (sca == null) {
            // allocated container no longer needed, add release request
            LOG.warn("Container {} allocated but nothing to deploy, going to release this container.", allocatedContainer.getId());
            releasedContainers.add(allocatedContainer.getId());
          } else {
            AllocatedContainer allocatedContainerHolder = new AllocatedContainer(allocatedContainer);
            stats.fetchAllocatedContainersMap().put(allocatedContainer.getId().toString(), allocatedContainerHolder);
            ByteBuffer tokens = null;
            if (UserGroupInformation.isSecurityEnabled()) {
              UserGroupInformation ugi = UserGroupInformation.getLoginUser();
              Token<StramDelegationTokenIdentifier> delegationToken = allocateDelegationToken(ugi.getUserName(), heartbeatListener.getAddress());
              allocatedContainerHolder.delegationToken = delegationToken;
              //ByteBuffer tokens = LaunchContainerRunnable.getTokens(delegationTokenManager, heartbeatListener.getAddress());
              tokens = getTokens(ugi, delegationToken);
            }
            IApexContainerLaunchContext apexContainerLaunchContext = new ApexContainerLaunchContext(allocatedContainer, nmClient);
            LaunchContainerRunnable launchContainer = new LaunchContainerRunnable(apexContainerLaunchContext, sca, tokens);
            // Thread launchThread = new Thread(runnableLaunchContainer);
            // launchThreads.add(launchThread);
            // launchThread.start();
            launchContainer.run(); // communication with NMs is now async

            // record container start event
            StramEvent ev = new StramEvent.StartContainerEvent(allocatedContainer.getId().toString(),
                allocatedContainer.getNodeId().toString(), groupingManager.getEventGroupIdForAffectedContainer(allocatedContainer.getId().toString()));
            ev.setTimestamp(timestamp);
            dnmgr.recordEventAsync(ev);
          }
        }

        // track node updates for future locality constraint allocations
        // TODO: it seems 2.0.4-alpha doesn't give us any updates
        resourceRequestor.updateNodeReports(amResp.getUpdatedNodes());

        // Check the completed containers
        List<ContainerStatus> completedContainers = amResp.getCompletedContainersStatuses();
        // LOG.debug("Got response from RM for container ask, completedCnt=" + completedContainers.size());
        List<String> blacklistAdditions = new ArrayList<>();
        for (ContainerStatus containerStatus : completedContainers) {
          LOG.info("Completed containerId=" + containerStatus.getContainerId() + ", state=" + containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());

          // non complete containers should not be here
          assert (containerStatus.getState() == ContainerState.COMPLETE);

          AllocatedContainer allocatedContainer = (AllocatedContainer)stats.fetchAllocatedContainersMap().remove(containerStatus.getContainerId().toString());
          if (allocatedContainer != null && allocatedContainer.delegationToken != null) {
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();
            delegationTokenManager.cancelToken(allocatedContainer.delegationToken, ugi.getUserName());
          }
          EventGroupId groupId = null;
          int exitStatus = containerStatus.getExitStatus();
          if (0 != exitStatus) {
            if (allocatedContainer != null) {
              stats.fetchNumFailedContainers().incrementAndGet();
              if (exitStatus != 1 && maxConsecutiveContainerFailures != Integer.MAX_VALUE) {
                // If container failure due to framework
                String hostname = allocatedContainer.container.getNodeId().getHost();
                if (!failedBlackListedNodes.contains(hostname)) {
                  // Blacklist the node if not already blacklisted
                  if (failedContainerNodesMap.containsKey(hostname)) {
                    NodeFailureStats stats = failedContainerNodesMap.get(hostname);
                    long timeStamp = System.currentTimeMillis();
                    if (timeStamp - stats.getLastFailureTimeStamp() >= blacklistRemovalTime) {
                      // Reset failure count if last failure was before Blacklist removal time
                      stats.setFailureCount(1);
                      stats.setLastFailureTimeStamp(timeStamp);
                    } else {
                      stats.setLastFailureTimeStamp(timeStamp);
                      stats.IncFailureCount();
                      if (stats.getFailureCount() >= maxConsecutiveContainerFailures) {
                        LOG.info("Node {} failed {} times consecutively within {} minutes, marking the node blacklisted", hostname, stats.getFailureCount(), blacklistRemovalTime / (60 * 1000));
                        blacklistAdditions.add(hostname);
                        failedBlackListedNodes.add(hostname);
                      }
                    }
                  } else {
                    failedContainerNodesMap.put(hostname, new NodeFailureStats(System.currentTimeMillis(), 1));
                  }
                }
              }
            }
//          if (exitStatus == 1) {
//            // non-recoverable StreamingContainer failure
//            appDone = true;
//            finalStatus = FinalApplicationStatus.FAILED;
//            dnmgr.shutdownDiagnosticsMessage = "Unrecoverable failure " + containerStatus.getContainerId();
//            LOG.info("Exiting due to: {}", dnmgr.shutdownDiagnosticsMessage);
//          }
//          else {
            // Recoverable failure or process killed (externally or via stop request by AM)
            // also occurs when a container was released by the application but never assigned/launched
            LOG.debug("Container {} failed or killed.", containerStatus.getContainerId());
            String containerIdStr = containerStatus.getContainerId().toString();
            dnmgr.scheduleContainerRestart(containerIdStr);
            groupId = groupingManager.getEventGroupIdForAffectedContainer(containerIdStr);
//          }
          } else {
            // container completed successfully
            numCompletedContainers.incrementAndGet();
            LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
            // Reset counter for node failure, if exists
            String hostname = allocatedContainer.container.getNodeId().getHost();
            NodeFailureStats stats = failedContainerNodesMap.get(hostname);
            if (stats != null) {
              stats.setFailureCount(0);
            }
          }

          String containerIdStr = containerStatus.getContainerId().toString();
          dnmgr.removeContainerAgent(containerIdStr);

          // record container stop event
          StramEvent ev = new StramEvent.StopContainerEvent(containerIdStr, containerStatus.getExitStatus(), groupId);
          ev.setReason(containerStatus.getDiagnostics());
          dnmgr.recordEventAsync(ev);
        }

        if (!blacklistAdditions.isEmpty()) {
          amRmClient.updateBlacklist(blacklistAdditions, null);
          long timeStamp = System.currentTimeMillis();
          for (String hostname : blacklistAdditions) {
            NodeFailureStats stats = failedContainerNodesMap.get(hostname);
            stats.setBlackListAdditionTime(timeStamp);
          }
        }
        if (dnmgr.isForcedShutdown()) {
          LOG.info("Forced shutdown due to {}", dnmgr.getShutdownDiagnosticsMessage());
          finalStatus = FinalApplicationStatus.FAILED;
          appDone = true;
        } else if (stats.fetchAllocatedContainersMap().isEmpty() && numRequestedContainers == 0 && dnmgr.getContainerStartRequests().isEmpty()) {
          LOG.debug("Exiting as no more containers are allocated or requested");
          finalStatus = FinalApplicationStatus.SUCCEEDED;
          appDone = true;
        }

        LOG.debug("Current application state: loop={}, appDone={}, requested={}, released={}, completed={}, failed={}, currentAllocated={}, dnmgr.containerStartRequests={}",
            loopCounter, appDone, numRequestedContainers, numReleasedContainers, numCompletedContainers, stats.fetchNumFailedContainers(), stats.getAllocatedContainers(), dnmgr.getContainerStartRequests());

        // monitor child containers
        dnmgr.monitorHeartbeat(waitForRecovery > 0);

        waitForRecovery = Math.max(waitForRecovery - 1, 0);
      }

      finishApplication(finalStatus);
    }
  }

  private void finishApplication(FinalApplicationStatus finalStatus) throws YarnException, IOException
  {
    LOG.info("Application completed. Signalling finish to RM");
    FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
    finishReq.setFinalApplicationStatus(finalStatus);

    if (finalStatus != FinalApplicationStatus.SUCCEEDED) {
      String diagnostics = "Diagnostics." + " completed=" + numCompletedContainers.get() + ", allocated=" + stats.getAllocatedContainers() + ", failed=" + stats.fetchNumFailedContainers().get();
      if (!StringUtils.isEmpty(dnmgr.getShutdownDiagnosticsMessage())) {
        diagnostics += "\n";
        diagnostics += dnmgr.getShutdownDiagnosticsMessage();
      }
      // YARN-208 - as of 2.0.1-alpha dropped by the RM
      finishReq.setDiagnostics(diagnostics);
      // expected termination of the master process
      // application status and diagnostics message are set above
    }
    LOG.info("diagnostics: " + finishReq.getDiagnostics());
    amRmClient.unregisterApplicationMaster(finishReq.getFinalApplicationStatus(), finishReq.getDiagnostics(), null);
  }

  private Token<StramDelegationTokenIdentifier> allocateDelegationToken(String username, InetSocketAddress address)
  {
    StramDelegationTokenIdentifier identifier = new StramDelegationTokenIdentifier(new Text(username), new Text(""), new Text(""));
    String service = address.getAddress().getHostAddress() + ":" + address.getPort();
    Token<StramDelegationTokenIdentifier> stramToken = new Token<>(identifier, delegationTokenManager);
    stramToken.setService(new Text(service));
    return stramToken;
  }

  /**
   * Check for containers that were allocated in a previous attempt.
   * If the containers are still alive, wait for them to check in via heartbeat.
   */
  private List<ContainerId> previouslyAllocatedContainers(List<Container> containersListByYarn)
  {
    List<ContainerId> containersToRelease = new ArrayList<>();

    if (containersListByYarn.size() != 0) {

      LOG.debug("Containers list by YARN - {}", containersListByYarn);
      LOG.debug("Containers list by Streaming Container Manger - {}", dnmgr.getPhysicalPlan().getContainers());

      Map<String, Container> fromYarn = new HashMap<>();

      for (Container container : containersListByYarn) {
        fromYarn.put(container.getId().toString(), container);
      }

      for (PTContainer ptContainer : dnmgr.getPhysicalPlan().getContainers()) {

        String containerId = ptContainer.getExternalId();

        // SCM starts the container without external ID.
        if (containerId == null) {
          continue;
        }

        Container container = fromYarn.get(containerId);

        if (container != null) {
          stats.fetchAllocatedContainersMap().put(containerId, new AllocatedContainer(container));
          fromYarn.remove(containerId);
        } else {
          dnmgr.scheduleContainerRestart(containerId);
        }
      }

      for (Container container : fromYarn.values()) {
        containersToRelease.add(container.getId());
      }

      if (fromYarn.size() != 0) {
        LOG.info("Containers list returned by YARN, has the following container(s) which are not present in PhysicalPlan {}", fromYarn);
      }
    } else {
      dnmgr.deployAfterRestart();
    }

    return containersToRelease;
  }

  /**
   * Ask RM to allocate given no. of containers to this Application Master
   *
   * @param containerRequests        Containers to ask for from RM
   * @param removedContainerRequests Container requests to be removed
   * @param releasedContainers
   * @return Response from RM to AM with allocated containers
   * @throws YarnException
   */
  private AllocateResponse sendContainerAskToRM(List<ContainerRequest> containerRequests, List<ContainerRequest> removedContainerRequests, List<ContainerId> releasedContainers) throws YarnException, IOException
  {
    if (removedContainerRequests.size() > 0) {
      LOG.debug("Removing container request: {}", removedContainerRequests);
      for (ContainerRequest cr : removedContainerRequests) {
        amRmClient.removeContainerRequest(cr);
      }
    }
    if (containerRequests.size() > 0) {
      LOG.debug("Asking RM for containers: {}", containerRequests);
      for (ContainerRequest cr : containerRequests) {
        amRmClient.addContainerRequest(cr);
      }
    }

    for (ContainerId containerId : releasedContainers) {
      LOG.info("Released container, id={}", containerId.getId());
      amRmClient.releaseAssignedContainer(containerId);
    }

    for (String containerIdStr : dnmgr.getContainerStopRequests().values()) {
      IAllocatedContainer allocatedContainer = stats.fetchAllocatedContainersMap().get(containerIdStr);
      if (allocatedContainer != null) {
        allocatedContainer.stop();
      }
      dnmgr.getContainerStopRequests().remove(containerIdStr);
    }

    for (Map.Entry<String, IAllocatedContainer> entry : stats.fetchAllocatedContainersMap().entrySet()) {
      if (entry.getValue().checkStopRequestedTimeout()) {
        LOG.info("Timeout happened for NodeManager kill container request, recovering the container {} without waiting", entry.getKey());
        recoverHelper(entry.getKey());
      }
    }
    return amRmClient.allocate(0);
  }

  private class NMCallbackHandler implements NMClientAsync.CallbackHandler
  {
    NMCallbackHandler()
    {
    }

    @Override
    public void onContainerStopped(ContainerId containerId)
    {
      LOG.debug("Succeeded to stop Container {}", containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus)
    {
      LOG.debug("Container Status: id={}, status={}", containerId, containerStatus);
      if (containerStatus.getState() != ContainerState.RUNNING) {
        recoverContainer(containerId);
      }
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse)
    {
      LOG.debug("Succeeded to start Container {}", containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t)
    {
      LOG.error("Start container failed for: containerId={}", containerId, t);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t)
    {
      LOG.error("Failed to query the status of {}", containerId, t);
      // if the NM is not reachable, consider container lost and recover (occurs during AM recovery)
      recoverContainer(containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t)
    {
      LOG.warn("Failed to stop container {}", containerId, t);
      // container could not be stopped, we won't receive a stop event from AM heartbeat
      // short circuit and schedule recovery directly
      recoverContainer(containerId);
    }

    private void recoverContainer(final ContainerId containerId)
    {
      pendingTasks.add(new Runnable()
      {
        @Override
        public void run()
        {
          recoverHelper(containerId.toString());
        }
      });
    }
  }

  private void recoverHelper(final String containerId)
  {
    dnmgr.scheduleContainerRestart(containerId);
    stats.fetchAllocatedContainersMap().remove(containerId);
  }

  private class AllocatedContainer implements IAllocatedContainer
  {
    private final Container container;
    private long stop = Long.MAX_VALUE;

    private Token<StramDelegationTokenIdentifier> delegationToken;

    private AllocatedContainer(Container c)
    {
      container = c;
    }

    @Override
    public void stop()
    {
      if (stop == Long.MAX_VALUE) {
        nmClient.stopContainerAsync(container.getId(), container.getNodeId());
        LOG.info("Requested stop container {}", container.getId().toString());
        stop = System.currentTimeMillis();
      }
    }

    @Override
    public boolean checkStopRequestedTimeout()
    {
      return System.currentTimeMillis() - stop > REMOVE_CONTAINER_TIMEOUT;
    }
  }

  private static ByteBuffer getTokens(UserGroupInformation ugi, Token<StramDelegationTokenIdentifier> delegationToken)
  {
    try {
      Collection<Token<? extends TokenIdentifier>> tokens = ugi.getCredentials().getAllTokens();
      Credentials credentials = new Credentials();
      for (Token<? extends TokenIdentifier> token : tokens) {
        if (!token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
          credentials.addToken(token.getService(), token);
          LOG.debug("Passing container token {}", token);
        }
      }
      credentials.addToken(delegationToken.getService(), delegationToken);
      DataOutputBuffer dataOutput = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dataOutput);
      byte[] tokenBytes = dataOutput.getData();
      ByteBuffer cTokenBuf = ByteBuffer.wrap(tokenBytes);
      return cTokenBuf.duplicate();
    } catch (IOException e) {
      throw new RuntimeException("Error generating delegation token", e);
    }
  }
}
