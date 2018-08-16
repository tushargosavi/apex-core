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
package com.datatorrent.stram.security;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.apex.engine.api.security.TokenManager;

import com.datatorrent.stram.webapp.WebServices;

/**
 * Based on org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
 * See https://issues.apache.org/jira/browse/YARN-1516
 *
 * @since 0.9.2
 */
public class StramWSFilter implements Filter
{
  private static final Logger logger = LoggerFactory.getLogger(StramWSFilter.class);

  public static final String PROXY_HOST = "PROXY_HOST";
  public static final String PROXY_DELIMITER = ",";
  //update the proxy IP list about every 5 min
  private static final long updateInterval = 5 * 60 * 1000;

  public static final String CLIENT_COOKIE = "dt-client";

  // This will not be needed once all requests can go through the proxy
  private static final String WEBAPP_PROXY_USER = "proxy-user";

  private String[] proxyHosts;
  private Set<String> proxyAddresses = null;
  private long lastUpdate;

  private TokenManager tokenManager;

  @Override
  public void init(FilterConfig conf) throws ServletException
  {
    String proxy = conf.getInitParameter(PROXY_HOST);
    proxyHosts = proxy.split(PROXY_DELIMITER);
    tokenManager = ClusterProviderFactory.getProvider().getTokenManager();
    tokenManager.setup(null);
  }

  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  protected Set<String> getProxyAddresses() throws ServletException
  {
    long now = System.currentTimeMillis();
    synchronized (this) {
      if (proxyAddresses == null || (lastUpdate + updateInterval) >= now) {
        proxyAddresses = new HashSet<>();
        for (String proxyHost : proxyHosts) {
          try {
            logger.debug("resolving proxy hostname {}", proxyHost);
            for (InetAddress add : InetAddress.getAllByName(proxyHost)) {
              logger.debug("proxy address is: {}", add.getHostAddress());
              proxyAddresses.add(add.getHostAddress());
            }
            lastUpdate = now;
          } catch (UnknownHostException e) {
            throw new ServletException("Could not locate " + proxyHost, e);
          }
        }
      }
      return proxyAddresses;
    }
  }

  @Override
  public void destroy()
  {
    tokenManager.teardown();
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException, ServletException
  {
    if (!(req instanceof HttpServletRequest)) {
      throw new ServletException("This filter only works for HTTP/HTTPS");
    }

    HttpServletRequest httpReq = (HttpServletRequest)req;
    HttpServletResponse httpResp = (HttpServletResponse)resp;
    String remoteAddr = httpReq.getRemoteAddr();
    String requestURI = httpReq.getRequestURI();
    boolean authenticate = true;
    String user = null;
    if (getProxyAddresses().contains(httpReq.getRemoteAddr())) {
      if (httpReq.getCookies() != null) {
        for (Cookie c : httpReq.getCookies()) {
          if (WEBAPP_PROXY_USER.equals(c.getName())) {
            user = c.getValue();
            break;
          }
        }
      }
      if (requestURI.equals(WebServices.PATH) && (user != null)) {
        String token = tokenManager.issueToken(user, httpReq.getLocalAddr());
        logger.debug("{}: creating token {}", remoteAddr, token);
        Cookie cookie = new Cookie(CLIENT_COOKIE, token);
        httpResp.addCookie(cookie);
      } else {
        logger.info("{}: proxy access to URI {} by user {}, no cookie created", remoteAddr, requestURI, user);
      }
      authenticate = false;
    }
    if (authenticate) {
      Cookie cookie = null;
      if (httpReq.getCookies() != null) {
        for (Cookie c : httpReq.getCookies()) {
          if (c.getName().equals(CLIENT_COOKIE)) {
            cookie = c;
            break;
          }
        }
      }
      boolean valid = false;
      if (cookie != null) {
        user = tokenManager.verifyToken(cookie.getValue(), remoteAddr);
        if (user != null) {
          valid = true;
        } else {
          logger.debug("{}: invalid cookie {}", remoteAddr, cookie.getValue());
        }
      } else {
        logger.debug("{}: cookie not found {}", remoteAddr, CLIENT_COOKIE);
      }
      if (!valid) {
        logger.debug("{}: auth failure", remoteAddr);
        httpResp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return;
      }
    }

    if (user == null) {
      logger.debug("{}: could not find user, so user principal will not be set", remoteAddr);
      chain.doFilter(req, resp);
    } else {
      final StramWSPrincipal principal = new StramWSPrincipal(user);
      ServletRequest requestWrapper = new StramWSServletRequestWrapper(httpReq, principal);
      chain.doFilter(requestWrapper, resp);
    }
  }
}
