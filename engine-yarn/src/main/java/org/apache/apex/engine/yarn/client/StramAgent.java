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

import javax.ws.rs.core.NewCookie;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.sun.jersey.api.client.ClientResponse;

import com.datatorrent.stram.security.StramWSFilter;
import com.datatorrent.stram.util.WebServicesClient;
import com.datatorrent.stram.webapp.WebServices;

public class StramAgent extends com.datatorrent.stram.client.StramAgent
{
  private static final Logger LOG = LoggerFactory.getLogger(com.datatorrent.stram.client.StramAgent.class);

  public StramAgent(FileSystem fs, Configuration conf)
  {
    super(fs, conf);
  }

  @Override
  protected StramWebServicesInfo retrieveWebServicesInfo(String appId)
  {
    String url;
    try (YarnClient yarnClient = StramClientUtils.createYarnClient(conf)) {
      ApplicationReport ar = yarnClient.getApplicationReport(ConverterUtils.toApplicationId(appId));
      if (ar == null) {
        LOG.warn("YARN does not have record for this application {}", appId);
        return null;
      } else if (ar.getYarnApplicationState() != YarnApplicationState.RUNNING) {
        LOG.debug("Application {} is not running (state: {})", appId, ar.getYarnApplicationState());
        return null;
      }

      String trackingUrl = ar.getTrackingUrl();
      if (!trackingUrl.startsWith("http://")
          && !trackingUrl.startsWith("https://")) {
        url = "http://" + trackingUrl;
      } else {
        url = trackingUrl;
      }
      if (StringUtils.isBlank(url)) {
        LOG.error("Cannot get tracking url from YARN");
        return null;
      }
      if (url.endsWith("/")) {
        url = url.substring(0, url.length() - 1);
      }
      url += WebServices.PATH;
    } catch (Exception ex) {
      LOG.error("Cannot retrieve web services info", ex);
      return null;
    }

    WebServicesClient webServicesClient = new WebServicesClient();
    try {
      JSONObject response;
      String secToken = null;
      ClientResponse clientResponse;
      int i = 0;
      while (true) {
        LOG.debug("Accessing url {}", url);
        clientResponse = webServicesClient.process(url,
            ClientResponse.class,
            new WebServicesClient.GetWebServicesHandler<ClientResponse>());
        String val = clientResponse.getHeaders().getFirst("Refresh");
        if (val == null) {
          break;
        }
        int index = val.indexOf("url=");
        if (index < 0) {
          break;
        }
        url = val.substring(index + 4);
        if (i++ > MAX_REDIRECTS) {
          LOG.error("Cannot get web service info -- exceeded the max number of redirects");
          return null;
        }
      }

      if (!UserGroupInformation.isSecurityEnabled()) {
        response = new JSONObject(clientResponse.getEntity(String.class));
      } else {
        if (UserGroupInformation.isSecurityEnabled()) {
          for (NewCookie nc : clientResponse.getCookies()) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Cookie " + nc.getName() + " " + nc.getValue());
            }
            if (nc.getName().equals(StramWSFilter.CLIENT_COOKIE)) {
              secToken = nc.getValue();
            }
          }
        }
        response = new JSONObject(clientResponse.getEntity(String.class));
      }
      String version = response.getString("version");
      response = webServicesClient.process(url + "/" + version + "/stram/info",
                                           JSONObject.class,
                                           new WebServicesClient.GetWebServicesHandler<JSONObject>());
      String appMasterUrl = response.getString("appMasterTrackingUrl");
      String appPath = response.getString("appPath");
      String user = response.getString("user");
      JSONObject permissionsInfo = null;
      Path permissionsPath = new Path(appPath, "permissions.json");
      LOG.debug("Checking for permission information in file {}", permissionsPath);
      try {
        if (fileSystem.exists(permissionsPath)) {
          LOG.info("Loading permission information");
          try (FSDataInputStream is = fileSystem.open(permissionsPath)) {
            permissionsInfo = new JSONObject(IOUtils.toString(is));
          }
          LOG.debug("Loaded permission file successfully");
        } else {
          // ignore and log messages if file is not found
          LOG.info("Permission information is not available as the application is not configured with it");
        }
      } catch (IOException ex) {
        // ignore and log message when unable to read the file
        LOG.info("Permission information is not available", ex);
      }
      return new StramWebServicesInfo(appMasterUrl, version, appPath, user, secToken, permissionsInfo);
    } catch (Exception ex) {
      LOG.warn("Cannot retrieve web service info for app {}", appId, ex);
      return null;
    }
  }
}
