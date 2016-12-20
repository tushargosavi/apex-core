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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.datatorrent.stram.client.StramAppLauncher.AppFactory;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;


/**
 * <p>
 * AppPackage class.</p>
 *
 * @since 1.0.3
 */
public class AppPackage extends JarFile
{
  public static final String ATTRIBUTE_DT_ENGINE_VERSION = "DT-Engine-Version";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_NAME = "DT-App-Package-Name";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_VERSION = "DT-App-Package-Version";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_GROUP_ID = "DT-App-Package-Group-Id";
  public static final String ATTRIBUTE_CLASS_PATH = "Class-Path";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_DISPLAY_NAME = "DT-App-Package-Display-Name";
  public static final String ATTRIBUTE_DT_APP_PACKAGE_DESCRIPTION = "DT-App-Package-Description";

  private final String appPackageName;
  private final String appPackageVersion;
  private final String appPackageGroupId;
  private final String dtEngineVersion;
  private final String appPackageDescription;
  private final String appPackageDisplayName;
  private final ArrayList<String> classPath = new ArrayList<>();
  private final File directory;

  private final List<AppInfo> applications = new ArrayList<>();
  private final List<String> appJars = new ArrayList<>();
  private final List<String> appJsonFiles = new ArrayList<>();
  private final List<String> appPropertiesFiles = new ArrayList<>();

  private final Set<String> requiredProperties = new TreeSet<>();
  private final Map<String, String> defaultProperties = new TreeMap<>();
  private final Set<String> configs = new TreeSet<>();
  private Configuration config;
  private final File resourcesDirectory;
  private final boolean cleanOnClose;

  public static class AppInfo
  {
    public final String name;
    public final String file;
    public final String type;
    public String displayName;
    public LogicalPlan dag;
    public String error;
    public String errorStackTrace;

    public Set<String> requiredProperties = new TreeSet<>();
    public Map<String, String> defaultProperties = new TreeMap<>();

    public AppInfo(String name, String file, String type)
    {
      this.name = name;
      this.file = file;
      this.type = type;
    }

  }

  public AppPackage(File file) throws IOException, ZipException
  {
    this(file, false);
  }

  /**
   * Creates an App Package object.
   *
   * If app directory is to be processed, there may be resource leak in the class loader. Only pass true for short-lived
   * applications
   *
   * If contentFolder is not null, it will try to create the contentFolder, file will be retained on disk after App Package is closed
   * If contentFolder is null, temp folder will be created and will be cleaned on close()
   *
   * @param file
   * @param contentFolder  the folder that the app package will be extracted to
   * @param processAppDirectory
   * @throws java.io.IOException
   * @throws net.lingala.zip4j.exception.ZipException
   */
  public AppPackage(File file, File contentFolder, boolean processAppDirectory) throws IOException, ZipException
  {
    super(file);

    config = StramClientUtils.addDTSiteResources(new YarnConfiguration());
    if (contentFolder != null) {
      FileUtils.forceMkdir(contentFolder);
      cleanOnClose = false;
    } else {
      cleanOnClose = true;
      contentFolder = Files.createTempDirectory("dt-appPackage-").toFile();
    }
    directory = contentFolder;

    Manifest manifest = getManifest();
    if (manifest == null) {
      throw new IOException("Not a valid app package. MANIFEST.MF is not present.");
    }
    Attributes attr = manifest.getMainAttributes();
    appPackageName = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_NAME);
    appPackageVersion = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_VERSION);
    appPackageGroupId = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_GROUP_ID);
    dtEngineVersion = attr.getValue(ATTRIBUTE_DT_ENGINE_VERSION);
    appPackageDisplayName = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_DISPLAY_NAME);
    appPackageDescription = attr.getValue(ATTRIBUTE_DT_APP_PACKAGE_DESCRIPTION);
    String classPathString = attr.getValue(ATTRIBUTE_CLASS_PATH);
    if (appPackageName == null || appPackageVersion == null || classPathString == null) {
      throw new IOException("Not a valid app package.  App Package Name or Version or Class-Path is missing from MANIFEST.MF");
    }
    classPath.addAll(Arrays.asList(StringUtils.split(classPathString, " ")));
    extractToDirectory(directory, file);

    File confDirectory = new File(directory, "conf");
    if (confDirectory.exists()) {
      processConfDirectory(confDirectory);
    }
    resourcesDirectory = new File(directory, "resources");

    File propertiesXml = new File(directory, "META-INF/properties.xml");
    if (propertiesXml.exists()) {
      LOG.info("Adding configuration file from {}", propertiesXml.getAbsolutePath());
      config.addResource(propertiesXml.getAbsolutePath());
      processPropertiesXml(propertiesXml, null);
    }

    if (processAppDirectory) {
      processAppDirectory(false);
    }
  }

  private void processAppProperties()
  {
    for (AppInfo app : applications) {
      app.requiredProperties.addAll(requiredProperties);
      app.defaultProperties.putAll(defaultProperties);
      File appPropertiesXml = new File(directory, "META-INF/properties-" + app.name + ".xml");
      if (appPropertiesXml.exists()) {
        processPropertiesXml(appPropertiesXml, app);
      }
    }
  }

  /**
   * Creates an App Package object.
   *
   * If app directory is to be processed, there may be resource leak in the class loader. Only pass true for short-lived
   * applications
   *
   * Files in app package will be extracted to tmp folder and will be cleaned on close()
   * The close() method could be explicitly called or implicitly called by GC finalize()
   *
   * @param file
   * @param processAppDirectory
   * @throws java.io.IOException
   * @throws net.lingala.zip4j.exception.ZipException
   */
  public AppPackage(File file, boolean processAppDirectory) throws IOException, ZipException
  {
    this(file, null, processAppDirectory);
  }

  public static void extractToDirectory(File directory, File appPackageFile) throws ZipException
  {
    ZipFile zipFile = new ZipFile(appPackageFile);

    if (zipFile.isEncrypted()) {
      throw new ZipException("Encrypted app package not supported yet");
    }

    directory.mkdirs();
    zipFile.extractAll(directory.getAbsolutePath());
  }

  public static void createAppPackageFile(File fileToBeCreated, File directory) throws ZipException
  {
    ZipFile zipFile = new ZipFile(fileToBeCreated);
    ZipParameters params = new ZipParameters();
    params.setIncludeRootFolder(false);
    zipFile.addFolder(directory, params);
  }

  public File tempDirectory()
  {
    return directory;
  }

  @Override
  public void close() throws IOException
  {
    super.close();
    if (cleanOnClose) {
      cleanContent();
    }
  }

  public void cleanContent() throws IOException
  {
    FileUtils.deleteDirectory(directory);
    LOG.debug("App Package {}-{} folder {} is removed", appPackageName, appPackageVersion, directory.getAbsolutePath());
  }

  public String getAppPackageName()
  {
    return appPackageName;
  }

  public String getAppPackageVersion()
  {
    return appPackageVersion;
  }

  public String getAppPackageGroupId()
  {
    return appPackageGroupId;
  }

  public String getAppPackageDescription()
  {
    return appPackageDescription;
  }

  public String getAppPackageDisplayName()
  {
    return appPackageDisplayName;
  }

  public String getDtEngineVersion()
  {
    return dtEngineVersion;
  }

  public List<String> getClassPath()
  {
    return Collections.unmodifiableList(classPath);
  }

  public Collection<String> getConfigs()
  {
    return Collections.unmodifiableCollection(configs);
  }

  public File resourcesDirectory()
  {
    return resourcesDirectory;
  }

  public List<AppInfo> getApplications()
  {
    return Collections.unmodifiableList(applications);
  }

  public List<String> getAppJars()
  {
    return Collections.unmodifiableList(appJars);
  }

  public List<String> getAppJsonFiles()
  {
    return Collections.unmodifiableList(appJsonFiles);
  }

  public List<String> getAppPropertiesFiles()
  {
    return Collections.unmodifiableList(appPropertiesFiles);
  }

  public Set<String> getRequiredProperties()
  {
    return Collections.unmodifiableSet(requiredProperties);
  }

  public Map<String, String> getDefaultProperties()
  {
    return Collections.unmodifiableMap(defaultProperties);
  }

  public void processAppDirectory(boolean skipJars)
  {
    File dir = new File(directory, "app");
    applications.clear();

    List<String> absClassPath = new ArrayList<>(classPath);
    for (int i = 0; i < absClassPath.size(); i++) {
      String path = absClassPath.get(i);
      if (!path.startsWith("/")) {
        absClassPath.set(i, directory + "/" + path);
      }
    }
    config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, StringUtils.join(absClassPath, ','));
    File[] files = dir.listFiles();
    for (File entry : files) {

      if (entry.getName().endsWith(".jar") && !skipJars) {
        appJars.add(entry.getName());
        try {
          StramAppLauncher stramAppLauncher = new StramAppLauncher(entry, config);
          stramAppLauncher.loadDependencies();
          List<AppFactory> appFactories = stramAppLauncher.getBundledTopologies();
          for (AppFactory appFactory : appFactories) {
            String appName = stramAppLauncher.getLogicalPlanConfiguration().getAppAlias(appFactory.getName());
            if (appName == null) {
              appName = appFactory.getName();
            }
            AppInfo appInfo = new AppInfo(appName, entry.getName(), "class");
            appInfo.displayName = appFactory.getDisplayName();
            try {
              LOG.info("appfactory instance is name {} {}", appInfo.displayName, appFactory.getClass().getName());
              appInfo.dag = appFactory.createApp(stramAppLauncher.getLogicalPlanConfiguration());
              appInfo.dag.validate();
            } catch (Throwable ex) {
              appInfo.error = ex.getMessage();
              appInfo.errorStackTrace = ExceptionUtils.getStackTrace(ex);
            }
            applications.add(appInfo);
          }
        } catch (Exception ex) {
          LOG.error("Caught exception trying to process {}", entry.getName(), ex);
        }
      }
    }

    // this is for the properties and json files to be able to depend on the app jars,
    // since it's possible for users to implement the operators as part of the app package
    for (String appJar : appJars) {
      absClassPath.add(new File(dir, appJar).getAbsolutePath());
    }
    config.set(StramAppLauncher.LIBJARS_CONF_KEY_NAME, StringUtils.join(absClassPath, ','));
    files = dir.listFiles();
    for (File entry : files) {
      if (entry.getName().endsWith(".json")) {
        appJsonFiles.add(entry.getName());
        AppInfo appInfo = StramClientUtils.jsonFileToAppInfo(entry, config);

        if (appInfo != null) {
          applications.add(appInfo);
        }

      } else if (entry.getName().endsWith(".properties")) {
        appPropertiesFiles.add(entry.getName());
        try {
          AppFactory appFactory = new StramAppLauncher.PropertyFileAppFactory(entry);
          StramAppLauncher stramAppLauncher = new StramAppLauncher(entry.getName(), config);
          stramAppLauncher.loadDependencies();
          AppInfo appInfo = new AppInfo(appFactory.getName(), entry.getName(), "properties");
          appInfo.displayName = appFactory.getDisplayName();
          try {
            appInfo.dag = appFactory.createApp(stramAppLauncher.getLogicalPlanConfiguration());
            appInfo.dag.validate();
          } catch (Throwable t) {
            appInfo.error = t.getMessage();
            appInfo.errorStackTrace = ExceptionUtils.getStackTrace(t);
          }
          applications.add(appInfo);
        } catch (Exception ex) {
          LOG.error("Caught exceptions trying to process {}", entry.getName(), ex);
        }
      } else if (!entry.getName().endsWith(".jar")) {
        LOG.warn("Ignoring file {} with unknown extension in app directory", entry.getName());
      }
    }

    processAppProperties();
  }

  private void processConfDirectory(File dir)
  {
    File[] files = dir.listFiles();
    for (File entry : files) {
      if (entry.getName().endsWith(".xml")) {
        configs.add(entry.getName());
      }
    }
  }

  private void processPropertiesXml(File file, AppInfo app)
  {
    LOG.info("processProperteis called ");
    DTConfiguration dtconfig = new DTConfiguration();
    try {
      dtconfig.loadFile(file);
      for (Map.Entry<String, String> entry : dtconfig) {
        LOG.info("property key {} value {}", entry.getKey(), entry.getValue());
        String key = entry.getKey();
        String value = entry.getValue();
        config.set(key, value);
        if (value == null) {
          if (app == null) {
            requiredProperties.add(key);
          } else {
            app.requiredProperties.add(key);
          }
        } else {
          if (app == null) {
            defaultProperties.put(key, value);
          } else {
            app.requiredProperties.remove(key);
            app.defaultProperties.put(key, value);
          }
        }
      }
    } catch (Exception ex) {
      LOG.warn("Ignoring META_INF/properties.xml because of error", ex);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(AppPackage.class);

}
