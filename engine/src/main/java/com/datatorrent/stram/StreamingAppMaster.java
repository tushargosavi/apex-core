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
package com.datatorrent.stram;

import java.io.StringWriter;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.apex.engine.StreamingAppMasterSetupContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.datatorrent.stram.debug.StdOutErrLog;
import com.datatorrent.stram.util.LoggerUtil;
import com.datatorrent.stram.util.VersionInfo;

/**
 * Entry point for Streaming Application Master
 * <p>
 *
 * @since 0.3.2
 */
public class StreamingAppMaster extends StramUtils.YarnContainerMain
{
  private static final Logger LOG = LoggerFactory.getLogger(StreamingAppMaster.class);

  /**
   * @param args
   *          Command line args
   * @throws Throwable
   */
  public static void main(final String[] args) throws Throwable
  {
    LoggerUtil.setupMDC("master");
    StdOutErrLog.tieSystemOutAndErrToLog();
    LOG.info("Master starting with classpath: {}", System.getProperty("java.class.path"));

    LOG.info("version: {}", VersionInfo.APEX_VERSION.getBuildVersion());
    StringWriter sw = new StringWriter();
    for (Map.Entry<String, String> e : System.getenv().entrySet()) {
      sw.append("\n").append(e.getKey()).append("=").append(e.getValue());
    }
    LOG.info("appmaster env:" + sw.toString());

    Options opts = new Options();
    opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");

    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    // option "help" overrides and cancels any run
    if (cliParser.hasOption("help")) {
      new HelpFormatter().printHelp("ApplicationMaster", opts);
      return;
    }

    boolean result = false;
    org.apache.apex.engine.api.StreamingAppMaster appMaster = null;
    try {
      // Look for a cluster provider
      appMaster = ClusterProviderFactory.getProvider().getStreamingAppMaster();
      LOG.info("Initializing Application Master.");

      appMaster.setup(new StreamingAppMasterSetupContext(cliParser));
      result = appMaster.run();
    } catch (Throwable t) {
      LOG.error("Exiting Application Master", t);
      System.exit(1);
    } finally {
      if (appMaster != null) {
        appMaster.teardown();
      }
    }

    if (result) {
      LOG.info("Application Master completed.");
      System.exit(0);
    } else {
      LOG.info("Application Master failed.");
      System.exit(2);
    }
  }

  public StreamingAppMaster()
  {
  }

}
