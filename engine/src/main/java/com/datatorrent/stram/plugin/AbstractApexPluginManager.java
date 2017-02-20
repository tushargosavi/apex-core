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
package com.datatorrent.stram.plugin;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.collect.Lists;

import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.plugin.ApexPlugin;
import com.datatorrent.stram.api.plugin.PluginContext;
import com.datatorrent.stram.api.plugin.PluginLocator;
import com.datatorrent.stram.api.plugin.PluginManager;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * A default implementation for ApexPluginManager. It handler common tasks such as per handler
 * registration. actual dispatching is left for classes extending from it.
 */
public abstract class AbstractApexPluginManager extends AbstractService implements ApexPluginManager
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractApexPluginManager.class);
  protected final Collection<ApexPlugin> plugins = Lists.newArrayList();
  protected final StramAppContext appContext;
  protected final StreamingContainerManager dmgr;
  private final PluginLocator locator;
  protected transient Configuration launchConfig;
  protected FileContext fileContext;
  protected final Map<ApexPlugin, PluginInfo> pluginInfoMap = new HashMap<>();
  protected PluginContext pluginContext;
  private long lastCommittedWindowId = Checkpoint.INITIAL_CHECKPOINT.getWindowId();

  public AbstractApexPluginManager(PluginLocator locator, StramAppContext context, StreamingContainerManager dmgr)
  {
    super(AbstractApexPluginManager.class.getName());
    this.locator = locator;
    this.appContext = context;
    this.dmgr = dmgr;
    LOG.info("Creating appex service ");
  }

  private Configuration readLaunchConfiguration() throws IOException
  {
    LOG.info("Reading launch configuration file ");
    Path appPath = new Path(appContext.getApplicationPath());
    URI uri = appPath.toUri();
    Configuration config = new YarnConfiguration();
    fileContext = uri.getScheme() == null ? FileContext.getFileContext(config) : FileContext.getFileContext(uri, config);
    FSDataInputStream is = fileContext.open(new Path(appPath, LogicalPlan.LAUNCH_CONFIG_FILE_NAME));
    config.addResource(is);
    LOG.info("Read launch configuration");
    return config;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception
  {
    super.serviceInit(conf);
    this.launchConfig = readLaunchConfiguration();
    pluginContext = new DefaultPluginContextImpl(appContext, dmgr, readLaunchConfiguration());
    if (locator != null) {
      Collection<ApexPlugin> plugins = locator.discoverPlugins();
      if (plugins != null) {
        this.plugins.addAll(plugins);
      }
    }

    super.serviceStart();
    for (ApexPlugin plugin : plugins) {
      plugin.init(new PluginManagerImpl(plugin));
    }
  }

  /**
   * Keeps information about plugin and its registrations. Dispatcher use this
   * information while delivering events to plugin.
   */
  class PluginInfo
  {
    final ApexPlugin plugin;
    final RegistrationMap registrations = new RegistrationMap();
    //final Map<PluginManager.RegistrationType<?>, List<PluginManager.Handler<?>>> registrations = new HashMap<>();
    public PluginInfo(ApexPlugin plugin)
    {
      this.plugin = plugin;
    }
  }

  PluginInfo getPluginInfo(ApexPlugin plugin)
  {
    PluginInfo pInfo = pluginInfoMap.get(plugin);
    if (pInfo == null) {
      pInfo = new PluginInfo(plugin);
      pluginInfoMap.put(plugin, pInfo);
    }
    return pInfo;
  }

  public <T> void register(PluginManager.RegistrationType<T> type, PluginManager.Handler<T> handler, ApexPlugin owner)
  {
    PluginInfo pInfo = getPluginInfo(owner);
    pInfo.registrations.put(type, handler);
  }

  @Override
  public void setCommittedWindowId(long committedWindowId)
  {
    if (lastCommittedWindowId != committedWindowId) {
      dispatch(PluginManager.COMMIT_EVENT, committedWindowId);
      lastCommittedWindowId = committedWindowId;
    }
  }

  /**
   * A wrapper PluginManager to track registration from a plugin. with this plugin
   * don't need to pass explicit owner argument during registration.
   */
  class PluginManagerImpl implements PluginManager
  {
    private final ApexPlugin owner;

    PluginManagerImpl(ApexPlugin plugin)
    {
      this.owner = plugin;
    }

    @Override
    public <T> void register(RegistrationType<T> type, Handler<T> handler)
    {
      AbstractApexPluginManager.this.register(type, handler, owner);
    }

    @Override
    public PluginContext getPluginContext()
    {
      return AbstractApexPluginManager.this.pluginContext;
    }
  }

  /**
   * A Map which keep track for registration from ApexPlugins.
   */
  class RegistrationMap
  {
    Map<PluginManager.RegistrationType<?>, List<?>> registrationMap = new HashMap<>();

    <T> void put(PluginManager.RegistrationType<T> registrationType, PluginManager.Handler<T> handler)
    {
      List<PluginManager.Handler<T>> handlers = get(registrationType);
      if (handlers == null) {
        handlers = new ArrayList<>();
        registrationMap.put(registrationType, handlers);
      }
      handlers.add(handler);
    }

    <T> List<PluginManager.Handler<T>> get(PluginManager.RegistrationType<T> registrationType)
    {
      return (List<PluginManager.Handler<T>>)registrationMap.get(registrationType);
    }
  }
}
