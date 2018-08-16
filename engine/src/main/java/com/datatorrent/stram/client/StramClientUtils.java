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
package com.datatorrent.stram.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.ClusterProviderFactory;
import org.apache.apex.engine.api.Settings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.security.StramUserLogin;
import com.datatorrent.stram.util.ConfigValidator;

/**
 * Collection of utility classes for command line interface package<p>
 * <br>
 * List includes<br>
 * Yarn Client Helper<br>
 * Resource Mgr Client Helper<br>
 * <br>
 *
 * @since 0.3.2
 */
public class StramClientUtils
{
  private static final Logger LOG = LoggerFactory.getLogger(StramClientUtils.class);

  public static final String DT_VERSION = StreamingApplication.DT_PREFIX + "version";
  public static final String DT_DFS_ROOT_DIR = StreamingApplication.DT_PREFIX + "dfsRootDirectory";
  public static final String APEX_APP_DFS_ROOT_DIR = StreamingApplication.APEX_PREFIX + "app.dfsRootDirectory";
  public static final String DT_DFS_USER_NAME = "%USER_NAME%";
  public static final String DT_CONFIG_STATUS = StreamingApplication.DT_PREFIX + "configStatus";
  public static final String SUBDIR_APPS = "apps";
  public static final String SUBDIR_PROFILES = "profiles";
  public static final String SUBDIR_CONF = "conf";
  public static final long RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE = 10 * 1000;
  public static final String DT_HDFS_TOKEN_MAX_LIFE_TIME = StreamingApplication.DT_PREFIX + "namenode.delegation.token.max-lifetime";
  public static final String DT_HDFS_TOKEN_RENEW_INTERVAL = StreamingApplication.DT_PREFIX + "namenode.delegation.token.renew-interval";
  public static final String HDFS_TOKEN_MAX_LIFE_TIME = "dfs.namenode.delegation.token.max-lifetime";
  public static final String HDFS_TOKEN_RENEW_INTERVAL = "dfs.namenode.delegation.token.renew-interval";
  public static final String DT_RM_TOKEN_MAX_LIFE_TIME = StreamingApplication.DT_PREFIX + "resourcemanager.delegation.token.max-lifetime";
  public static final String DT_RM_TOKEN_RENEW_INTERVAL = StreamingApplication.DT_PREFIX + "resourcemanager.delegation.token.renew-interval";
  @Deprecated
  public static final String KEY_TAB_FILE = StramUserLogin.DT_AUTH_PREFIX + "store.keytab";
  public static final String TOKEN_ANTICIPATORY_REFRESH_FACTOR = StramUserLogin.DT_AUTH_PREFIX + "token.refresh.factor";
  public static final long DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT = 7 * 24 * 60 * 60 * 1000;
  public static final long DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT = 24 * 60 * 60 * 1000;
  public static final String TOKEN_REFRESH_PRINCIPAL = StramUserLogin.DT_AUTH_PREFIX + "token.refresh.principal";
  public static final String TOKEN_REFRESH_KEYTAB = StramUserLogin.DT_AUTH_PREFIX + "token.refresh.keytab";
  /**
   * TBD<p>
   * <br>
   */

  public static String getHostName()
  {
    try {
      return java.net.InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
      return null;
    }
  }

  public static File getUserDTDirectory()
  {
    String envHome = System.getenv("HOME");
    if (StringUtils.isEmpty(envHome)) {
      return new File(FileUtils.getUserDirectory(), ".dt");
    } else {
      return new File(envHome, ".dt");
    }
  }

  public static File getConfigDir()
  {
    URL resource = StramClientUtils.class.getClassLoader().getResource(DT_ENV_SH_FILE);
    try {
      if (resource == null) {
        return getUserDTDirectory();
      }
      return new File(resource.toURI()).getParentFile();
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static File getInstallationDir()
  {
    URL resource = StramClientUtils.class.getClassLoader().getResource(DT_ENV_SH_FILE);
    try {
      if (resource == null) {
        return null;
      }
      return new File(resource.toURI()).getParentFile().getParentFile();
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static boolean isDevelopmentMode()
  {
    return getUserDTDirectory().equals(getConfigDir());
  }

  public static File getBackupsDirectory()
  {
    return new File(getConfigDir(), BACKUPS_DIRECTORY);
  }

  public static final String DT_DEFAULT_XML_FILE = "dt-default.xml";
  public static final String DT_SITE_XML_FILE = "dt-site.xml";
  public static final String DT_SITE_GLOBAL_XML_FILE = "dt-site-global.xml";
  public static final String DT_ENV_SH_FILE = "dt-env.sh";
  public static final String CUSTOM_ENV_SH_FILE = "custom-env.sh";
  public static final String BACKUPS_DIRECTORY = "backups";

  public static Configuration addDTDefaultResources(Configuration conf)
  {
    conf.addResource(DT_DEFAULT_XML_FILE);
    return conf;
  }

  public static Configuration addDTSiteResources(Configuration conf)
  {
    addDTLocalResources(conf);
    File targetGlobalFile;
    try (FileSystem fs = newFileSystemInstance(conf)) {
      // after getting the dfsRootDirectory config parameter, redo the entire process with the global config
      // load global settings from DFS
      targetGlobalFile = new File(String.format("%s/dt-site-global-%s.xml", System.getProperty("java.io.tmpdir"),
          UserGroupInformation.getLoginUser().getShortUserName()));
      org.apache.hadoop.fs.Path hdfsGlobalPath = new org.apache.hadoop.fs.Path(StramClientUtils.getDTDFSConfigDir(fs, conf), StramClientUtils.DT_SITE_GLOBAL_XML_FILE);
      LOG.debug("Copying global dt-site.xml from {} to {}", hdfsGlobalPath, targetGlobalFile.getAbsolutePath());
      fs.copyToLocalFile(hdfsGlobalPath, new org.apache.hadoop.fs.Path(targetGlobalFile.toURI()));
      addDTSiteResources(conf, targetGlobalFile);
      if (!isDevelopmentMode()) {
        // load node local config file
        addDTSiteResources(conf, new File(StramClientUtils.getConfigDir(), StramClientUtils.DT_SITE_XML_FILE));
      }
      // load user config file
      addDTSiteResources(conf, new File(StramClientUtils.getUserDTDirectory(), StramClientUtils.DT_SITE_XML_FILE));
    } catch (IOException ex) {
      // ignore
      LOG.debug("Caught exception when loading configuration: {}: moving on...", ex.getMessage());
    } finally {
      // Cannot delete the file here because addDTSiteResource which eventually calls Configuration.reloadConfiguration
      // does not actually reload the configuration.  The file is actually read later and it needs to exist.
      //
      //if (targetGlobalFile != null) {
      //targetGlobalFile.delete();
      //}
    }

    //Validate loggers-level settings
    String loggersLevel = conf.get(StramUtils.DT_LOGGERS_LEVEL);
    if (loggersLevel != null) {
      String[] targets = loggersLevel.split(",");
      Preconditions.checkArgument(targets.length > 0, "zero loggers level");
      for (String target : targets) {
        String[] parts = target.split(":");
        Preconditions.checkArgument(parts.length == 2, "incorrect " + target);
        Preconditions.checkArgument(ConfigValidator.validateLoggersLevel(parts[0], parts[1]), "incorrect " + target);
      }
    }
    convertDeprecatedProperties(conf);

    //
    // The ridiculous default RESOURCEMANAGER_CONNECT_MAX_WAIT_MS from hadoop is 15 minutes (!!!!), which actually translates to 20 minutes with the connect interval.
    // That means if there is anything wrong with YARN or if YARN is not running, the caller has to wait for up to 20 minutes until it gets an error.
    // We are overriding this to be 10 seconds maximum.
    //

    long rmConnectMaxWait = conf.getLong(Settings.Strings.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS.getValue(),
        Settings.Longs.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS.getValue());
    if (rmConnectMaxWait > RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE) {
      LOG.info("Overriding {} assigned value of {} to {} because the assigned value is too big.",
          Settings.Strings.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS.getValue(),
          rmConnectMaxWait, RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE);
      conf.setLong(Settings.Strings.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS.getValue(), RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE);
      long rmConnectRetryInterval = conf.getLong(Settings.Strings.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS.getValue(),
          Settings.Longs.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS.getValue());
      long defaultRetryInterval = Math.max(500, RESOURCEMANAGER_CONNECT_MAX_WAIT_MS_OVERRIDE / 5);
      if (rmConnectRetryInterval > defaultRetryInterval) {
        LOG.info("Overriding {} assigned value of {} to {} because the assigned value is too big.",
            Settings.Strings.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS.getValue(), rmConnectRetryInterval, defaultRetryInterval);
        conf.setLong(Settings.Strings.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS.getValue(), defaultRetryInterval);
      }
    }
    LOG.info(" conf object in stramclient {}", conf);
    return conf;
  }

  public static void addDTLocalResources(Configuration conf)
  {
    conf.addResource(DT_DEFAULT_XML_FILE);
    if (!isDevelopmentMode()) {
      addDTSiteResources(conf, new File(StramClientUtils.getConfigDir(), StramClientUtils.DT_SITE_XML_FILE));
    }
    addDTSiteResources(conf, new File(StramClientUtils.getUserDTDirectory(), StramClientUtils.DT_SITE_XML_FILE));
  }

  private static Configuration addDTSiteResources(Configuration conf, File confFile)
  {
    if (confFile.exists()) {
      LOG.info("Loading settings: " + confFile.toURI());
      conf.addResource(new Path(confFile.toURI()));
    } else {
      LOG.info("Configuration file {} is not found. Skipping...", confFile.toURI());
    }
    return conf;
  }

  @SuppressWarnings("deprecation")
  private static void convertDeprecatedProperties(Configuration conf)
  {
    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
    Map<String, String> newEntries = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (entry.getKey().startsWith("stram.")) {
        String newKey = StreamingApplication.DT_PREFIX + entry.getKey().substring(6);
        LOG.warn("Configuration property {} is deprecated. Please use {} instead.", entry.getKey(), newKey);
        newEntries.put(newKey, entry.getValue());
        iterator.remove();
      }
    }
    for (Map.Entry<String, String> entry : newEntries.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  public static URL getDTSiteXmlFile()
  {
    File cfgResource = new File(StramClientUtils.getConfigDir(), StramClientUtils.DT_SITE_XML_FILE);
    try {
      return cfgResource.toURI().toURL();
    } catch (MalformedURLException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static FileSystem newFileSystemInstance(Configuration conf) throws IOException
  {
    String dfsRootDir = conf.get(DT_DFS_ROOT_DIR);
    if (StringUtils.isBlank(dfsRootDir)) {
      return FileSystem.newInstance(conf);
    } else {
      if (dfsRootDir.contains(DT_DFS_USER_NAME)) {
        dfsRootDir = dfsRootDir.replace(DT_DFS_USER_NAME, UserGroupInformation.getLoginUser().getShortUserName());
        conf.set(DT_DFS_ROOT_DIR, dfsRootDir);
      }
      try {
        return FileSystem.newInstance(new URI(dfsRootDir), conf);
      } catch (URISyntaxException ex) {
        LOG.warn("{} is not a valid URI. Returning the default filesystem", dfsRootDir, ex);
        return FileSystem.newInstance(conf);
      }
    }
  }

  /**
   * Helper function used by both getApexDFSRootDir and getDTDFSRootDir to process dfsRootDir
   *
   * @param fs   FileSystem object for HDFS file system
   * @param conf  Configuration object
   * @param dfsRootDir  value of dt.dfsRootDir or apex.app.dfsRootDir
   * @param userShortName  current user short name (either login user or current user depending on impersonation settings)
   * @param prependHomeDir  prepend user's home dir if dfsRootDir is relative path

   * @return
   */
  private static Path evalDFSRootDir(FileSystem fs, Configuration conf, String dfsRootDir, String userShortName,
      boolean prependHomeDir)
  {
    try {
      if (userShortName != null && dfsRootDir.contains(DT_DFS_USER_NAME)) {
        dfsRootDir = dfsRootDir.replace(DT_DFS_USER_NAME, userShortName);
        conf.set(DT_DFS_ROOT_DIR, dfsRootDir);
      }
      URI uri = new URI(dfsRootDir);
      if (uri.isAbsolute()) {
        return new Path(uri);
      }
      if (userShortName != null && prependHomeDir && dfsRootDir.startsWith("/") == false) {
        dfsRootDir = "/user/" + userShortName + "/" + dfsRootDir;
      }
    } catch (URISyntaxException ex) {
      LOG.warn("{} is not a valid URI. Using the default filesystem to construct the path", dfsRootDir, ex);
    }
    return new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(), dfsRootDir);
  }

  private static String getDefaultRootFolder()
  {
    return "datatorrent";
  }

  /**
   * This gets the DFS Root dir to be used at runtime by Apex applications as per the following logic:
   * Value of apex.app.dfsRootDirectory is referred to as Apex-root-dir below.
   * A "user" refers to either impersonating or impersonated user:
   *    If apex.application.path.impersonated is true then use impersonated user else impersonating user.
   *
   * <ul>
   * <li> if Apex-root-dir is blank then just call getDTDFSRootDir to get the old behavior
   * <li> if Apex-root-dir value has %USER_NAME% in the string then replace it with the user's name, else use the absolute path as is.
   * <li> if Apex-root-dir value is a relative path then append it to the user's home directory.
   * </ul>
   *
   * @param fs FileSystem object for HDFS file system
   * @param conf  Configuration object
   * @return
   * @throws IOException
   */
  public static Path getApexDFSRootDir(FileSystem fs, Configuration conf)
  {
    String apexDfsRootDir = conf.get(APEX_APP_DFS_ROOT_DIR);
    boolean useImpersonated = conf.getBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);
    String userShortName = null;
    if (useImpersonated) {
      try {
        userShortName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ex) {
        LOG.warn("Error getting current/login user name {}", apexDfsRootDir, ex);
      }
    }
    if (!useImpersonated || userShortName == null) {
      return getDTDFSRootDir(fs, conf);
    }
    if (StringUtils.isBlank(apexDfsRootDir)) {
      apexDfsRootDir = getDefaultRootFolder();
    }
    return evalDFSRootDir(fs, conf, apexDfsRootDir, userShortName, true);
  }

  public static Path getDTDFSRootDir(FileSystem fs, Configuration conf)
  {
    String dfsRootDir = conf.get(DT_DFS_ROOT_DIR);
    if (StringUtils.isBlank(dfsRootDir)) {
      return new Path(fs.getHomeDirectory(), getDefaultRootFolder());
    }
    String userShortName = null;
    try {
      userShortName = UserGroupInformation.getLoginUser().getShortUserName();
    } catch (IOException ex) {
      LOG.warn("Error getting user login name {}", dfsRootDir, ex);
    }
    return evalDFSRootDir(fs, conf, dfsRootDir, userShortName, false);
  }

  public static Path getDTDFSConfigDir(FileSystem fs, Configuration conf)
  {
    return new Path(getDTDFSRootDir(fs, conf), SUBDIR_CONF);
  }

  public static Path getDTDFSProfilesDir(FileSystem fs, Configuration conf)
  {
    return new Path(getDTDFSRootDir(fs, conf), SUBDIR_PROFILES);
  }

  /**
   * Change DT environment variable in the env file.
   * Calling this will require a restart for the new setting to take place
   *
   * @param key
   * @param value
   * @throws IOException
   */
  public static void changeDTEnvironment(String key, String value) throws IOException
  {
    if (isDevelopmentMode()) {
      throw new IllegalStateException("Cannot change DT environment in development mode.");
    }
    URL resource = StramClientUtils.class.getClassLoader().getResource(CUSTOM_ENV_SH_FILE);
    if (resource == null) {
      File envFile = new File(StramClientUtils.getUserDTDirectory(), StramClientUtils.CUSTOM_ENV_SH_FILE);
      try (FileOutputStream out = new FileOutputStream(envFile)) {
        out.write(("export " + key + "=\"" + value + "\"\n").getBytes());
      }
    } else {
      try {
        File cfgResource = new File(resource.toURI());
        synchronized (StramClientUtils.class) {
          StringBuilder sb = new StringBuilder(1024);
          try (BufferedReader br = new BufferedReader(new FileReader(cfgResource))) {
            String line;
            boolean changed = false;
            while ((line = br.readLine()) != null) {
              try {
                line = line.trim();
                if (line.startsWith("#")) {
                  continue;
                }
                if (line.matches("export\\s+" + key + "=.*")) {
                  line = "export " + key + "=\"" + value + "\"";
                  changed = true;
                }
              } finally {
                sb.append(line).append("\n");
              }
            }
            if (!changed) {
              sb.append("export ").append(key).append("=\"").append(value).append("\"\n");
            }
          }
          if (sb.length() > 0) {
            try (FileOutputStream out = new FileOutputStream(cfgResource)) {
              out.write(sb.toString().getBytes());
            }
          }
        }
      } catch (URISyntaxException ex) {
        LOG.error("Caught exception when getting env resource:", ex);
      }
    }
  }

  public static void copyFromLocalFileNoChecksum(FileSystem fs, File fromLocal, Path toDFS) throws IOException
  {
    // This is to void the hadoop FileSystem API to perform checksum on the local file
    // This "feature" has caused a lot of headache because the local file can be copied from HDFS and modified,
    // and the checksum will fail if the file is again copied to HDFS
    try {
      new File(fromLocal.getParentFile(), "." + fromLocal.getName() + ".crc").delete();
    } catch (Exception ex) {
      // ignore
    }
    fs.copyFromLocalFile(new Path(fromLocal.toURI()), toDFS);
  }

  public static boolean configComplete(Configuration conf)
  {
    String configStatus = conf.get(StramClientUtils.DT_CONFIG_STATUS);
    return "complete".equals(configStatus);
  }

  public static void evalProperties(Properties target, Configuration vars)
  {
    ScriptEngine engine = new ScriptEngineManager().getEngineByName("javascript");

    Pattern substitutionPattern = Pattern.compile("\\$\\{(.+?)\\}");
    Pattern evalPattern = Pattern.compile("\\{% (.+?) %\\}");

    try {
      engine.eval("var _prop = {}");
      for (Map.Entry<String, String> entry : vars) {
        String evalString = String.format("_prop[\"%s\"] = \"%s\"", StringEscapeUtils.escapeJava(entry.getKey()), StringEscapeUtils.escapeJava(entry.getValue()));
        engine.eval(evalString);
      }
    } catch (ScriptException ex) {
      LOG.warn("Javascript error: {}", ex.getMessage());
    }

    for (Map.Entry<Object, Object> entry : target.entrySet()) {
      String value = entry.getValue().toString();

      Matcher matcher = substitutionPattern.matcher(value);
      if (matcher.find()) {
        StringBuilder newValue = new StringBuilder();
        int cursor = 0;
        do {
          newValue.append(value.substring(cursor, matcher.start()));
          String subst = vars.get(matcher.group(1));
          if (subst != null) {
            newValue.append(subst);
          }
          cursor = matcher.end();
        } while (matcher.find());
        newValue.append(value.substring(cursor));
        target.put(entry.getKey(), newValue.toString());
      }

      matcher = evalPattern.matcher(value);
      if (matcher.find()) {
        StringBuilder newValue = new StringBuilder();
        int cursor = 0;
        do {
          newValue.append(value.substring(cursor, matcher.start()));
          try {
            Object result = engine.eval(matcher.group(1));
            String eval = result.toString();

            if (eval != null) {
              newValue.append(eval);
            }
          } catch (ScriptException ex) {
            LOG.warn("JavaScript exception {}", ex.getMessage());
          }
          cursor = matcher.end();
        } while (matcher.find());
        newValue.append(value.substring(cursor));
        target.put(entry.getKey(), newValue.toString());
      }
    }
  }

  public static void evalConfiguration(Configuration conf)
  {
    Properties props = new Properties();
    for (Map.Entry entry : conf) {
      props.put(entry.getKey(), entry.getValue());
    }
    evalProperties(props, conf);
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      conf.set((String)entry.getKey(), (String)entry.getValue());
    }
  }

  public static <T> T doAs(String userName, PrivilegedExceptionAction<T> action) throws Exception
  {
    if (StringUtils.isNotBlank(userName) && !userName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
      LOG.info("Executing command as {}", userName);
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(userName, UserGroupInformation.getLoginUser());
      return ugi.doAs(action);
    } else {
      LOG.info("Executing command as if there is no login info: {}", userName);
      return action.run();
    }
  }

  public static String getSocketConnectString(InetSocketAddress socketAddress)
  {
    String host;
    InetAddress address = socketAddress.getAddress();
    if (address == null) {
      host = socketAddress.getHostString();
    } else if (address.isAnyLocalAddress() || address.isLoopbackAddress()) {
      host = address.getCanonicalHostName();
    } else {
      host = address.getHostName();
    }
    return host + ":" + socketAddress.getPort();
  }

  public static AppPackage.AppInfo jsonFileToAppInfo(File file, Configuration config)
  {
    AppPackage.AppInfo appInfo = null;

    try {
      StramAppLauncher.AppFactory appFactory = new StramAppLauncher.JsonFileAppFactory(file);
      StramAppLauncher stramAppLauncher = ClusterProviderFactory.getProvider().getStramAppLauncher(file.getName(), config);
      stramAppLauncher.loadDependencies();
      appInfo = new AppPackage.AppInfo(appFactory.getName(), file.getName(), "json");
      appInfo.displayName = appFactory.getDisplayName();
      try {
        appInfo.dag = appFactory.createApp(stramAppLauncher.getLogicalPlanConfiguration());
        appInfo.dag.validate();
      } catch (Exception ex) {
        appInfo.error = ex.getMessage();
        appInfo.errorStackTrace = ExceptionUtils.getStackTrace(ex);
      }
    } catch (Exception ex) {
      LOG.error("Caught exceptions trying to process {}", file.getName(), ex);
    }

    return appInfo;
  }

  public static void addAttributeToArgs(Attribute<String> attribute, Context context, List<CharSequence> vargs)
  {
    String value = context.getValue(attribute);
    if (value != null) {
      vargs.add(String.format("-D%s=$'%s'", attribute.getLongName(),
          value.replace("\\", "\\\\\\\\").replaceAll("['\"$]", "\\\\$0")));
    }
  }
}
