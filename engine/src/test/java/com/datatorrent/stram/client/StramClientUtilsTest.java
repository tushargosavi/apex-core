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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.datatorrent.stram.security.StramUserLogin;


/**
 * Unit tests for StramClientUtils
 */
public class StramClientUtilsTest
{

  @Test
  public void testEvalExpression() throws Exception
  {
    Configuration conf = new Configuration();
    conf.set("a.b.c", "123");
    conf.set("d.e.f", "456");
    conf.set("x.y.z", "foobar");

    Properties prop = new Properties();
    prop.put("product.result", "Product result is {% (_prop[\"a.b.c\"] * _prop[\"d.e.f\"]).toFixed(0) %}...");
    prop.put("concat.result", "Concat result is {% _prop[\"x.y.z\"] %} ... {% _prop[\"a.b.c\"] %} blah");

    StramClientUtils.evalProperties(prop, conf);

    Assert.assertEquals("Product result is " + (123 * 456) + "...", prop.get("product.result"));
    Assert.assertEquals("Concat result is foobar ... 123 blah", prop.get("concat.result"));
    Assert.assertEquals("123", conf.get("a.b.c"));
    Assert.assertEquals("456", conf.get("d.e.f"));
    Assert.assertEquals("foobar", conf.get("x.y.z"));
  }

  @Test
  public void testVariableSubstitution() throws Exception
  {
    Configuration conf = new Configuration();
    conf.set("a.b.c", "123");
    conf.set("x.y.z", "foobar");

    Properties prop = new Properties();
    prop.put("var.result", "1111 ${a.b.c} xxx ${x.y.z} yyy");

    StramClientUtils.evalProperties(prop, conf);

    Assert.assertEquals("1111 123 xxx foobar yyy", prop.get("var.result"));
  }

  @Test
  public void testEvalConfiguration() throws Exception
  {
    Configuration conf = new Configuration();
    conf.set("a.b.c", "123");
    conf.set("x.y.z", "foobar");
    conf.set("sub.result", "1111 ${a.b.c} xxx ${x.y.z} yyy");
    conf.set("script.result", "1111 {% (_prop[\"a.b.c\"] * _prop[\"a.b.c\"]).toFixed(0) %} xxx");

    StramClientUtils.evalConfiguration(conf);

    Assert.assertEquals("1111 123 xxx foobar yyy", conf.get("sub.result"));
    Assert.assertEquals("1111 15129 xxx", conf.get("script.result"));
  }

  protected String getHostString(String host) throws UnknownHostException
  {
    InetAddress address = InetAddress.getByName(host);
    if (address.isAnyLocalAddress() || address.isLoopbackAddress()) {
      return address.getCanonicalHostName();
    } else {
      return address.getHostName();
    }
  }

  /**
   * apex.dfsRootDirectory not set: legacy behavior of getDTDFSRootDir()
   * @throws IOException
   *
   */
  @Test
  public void getApexDFSRootDirLegacy() throws IOException
  {
    Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.DT_DFS_ROOT_DIR, "/a/b/c");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);

    FileSystem fs = FileSystem.newInstance(conf);
    Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
    Assert.assertEquals("file:/a/b/c", path.toString());
  }

  /**
   * apex.dfsRootDirectory set: absolute path e.g. /x/y/z
   * @throws IOException
   *
   */
  @Test
  public void getApexDFSRootDirAbsPath() throws IOException
  {
    Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "/x/y/z");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);

    FileSystem fs = FileSystem.newInstance(conf);
    UserGroupInformation testUser = UserGroupInformation.createUserForTesting("testUser1", new String[]{""});
    UserGroupInformation.setLoginUser(testUser);
    Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
    Assert.assertEquals(fs.getHomeDirectory() + "/datatorrent", path.toString());
  }

  /**
   * apex.dfsRootDirectory set: absolute path with scheme e.g. file:/p/q/r
   * @throws IOException
   *
   */
  @Test
  public void getApexDFSRootDirScheme() throws IOException
  {
    Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "file:/p/q/r");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);

    FileSystem fs = FileSystem.newInstance(conf);
    UserGroupInformation testUser = UserGroupInformation.createUserForTesting("testUser1", new String[]{""});
    UserGroupInformation.setLoginUser(testUser);
    Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
    Assert.assertEquals(fs.getHomeDirectory() + "/datatorrent", path.toString());
  }

  /**
   * apex.dfsRootDirectory set: absolute path with variable %USER_NAME%
   * @throws IOException
   * @throws InterruptedException
   *
   */
  @Test
  public void getApexDFSRootDirWithVar() throws IOException, InterruptedException
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "/x/%USER_NAME%/z");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);

    final FileSystem fs = FileSystem.newInstance(conf);
    UserGroupInformation testUser = UserGroupInformation.createUserForTesting("testUser1", new String[]{""});
    UserGroupInformation.setLoginUser(testUser);
    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("impersonated", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals(fs.getHomeDirectory() + "/datatorrent", path.toString());
        return null;
      }
    });
  }

  /**
   * apex.dfsRootDirectory set: absolute path with %USER_NAME% and scheme e.g. file:/x/%USER_NAME%/z
   * @throws IOException
   * @throws InterruptedException
   *
   */
  @Test
  public void getApexDFSRootDirWithSchemeAndVar() throws IOException, InterruptedException
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "file:/x/%USER_NAME%/z");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true);

    final FileSystem fs = FileSystem.newInstance(conf);
    UserGroupInformation testUser = UserGroupInformation.createUserForTesting("testUser1", new String[]{""});
    UserGroupInformation.setLoginUser(testUser);
    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("impersonated", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals("file:/x/impersonated/z", path.toString());
        return null;
      }
    });
  }

  /**
   * apex.dfsRootDirectory set: relative path
   * @throws IOException
   * @throws InterruptedException
   *
   */
  @Test
  public void getApexDFSRootDirRelPath() throws IOException, InterruptedException
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "apex");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, false);

    final FileSystem fs = FileSystem.newInstance(conf);
    UserGroupInformation testUser = UserGroupInformation.createUserForTesting("testUser1", new String[]{""});
    UserGroupInformation.setLoginUser(testUser);
    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("impersonated", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals(fs.getHomeDirectory() + "/datatorrent", path.toString());
        return null;
      }
    });
  }

  /**
   * apex.dfsRootDirectory set: absolute path with %USER_NAME% and impersonation enabled
   * @throws IOException
   * @throws InterruptedException
   *
   */
  @Test
  public void getApexDFSRootDirAbsPathAndVar() throws IOException, InterruptedException
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "/x/%USER_NAME%/z");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true);

    final FileSystem fs = FileSystem.newInstance(conf);
    UserGroupInformation testUser = UserGroupInformation.createUserForTesting("testUser1", new String[]{""});
    UserGroupInformation.setLoginUser(testUser);
    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("impersonated", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals("file:/x/impersonated/z", path.toString());
        return null;
      }
    });
  }

  /**
   * apex.dfsRootDirectory set: relative path and impersonation enabled and doAS
   * @throws IOException
   * @throws InterruptedException
   *
   */
  @Test
  public void getApexDFSRootDirRelPathAndImpersonation() throws IOException, InterruptedException
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "apex");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true);

    final FileSystem fs = FileSystem.newInstance(conf);
    UserGroupInformation testUser = UserGroupInformation.createUserForTesting("testUser1", new String[]{""});
    UserGroupInformation.setLoginUser(testUser);
    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("testUser2", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals("file:/user/testUser2/apex", path.toString());
        return null;
      }
    });
  }

  /**
   * apex.dfsRootDirectory set: relative path blank and impersonation enabled and doAS
   * @throws IOException
   * @throws InterruptedException
   *
   */
  @Test
  public void getApexDFSRootDirBlankPathAndImpersonation() throws IOException, InterruptedException
  {
    final Configuration conf = new Configuration(false);
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true);

    final FileSystem fs = FileSystem.newInstance(conf);
    UserGroupInformation testUser = UserGroupInformation.createUserForTesting("testUser1", new String[]{""});
    UserGroupInformation.setLoginUser(testUser);
    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("testUser2", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals("file:/user/testUser2/datatorrent", path.toString());
        return null;
      }
    });
  }

  /**
   * apex.dfsRootDirectory set: relative path having %USER_NAME% and impersonation enabled and doAS
   * Make sure currentUser appears twice
   * @throws IOException
   * @throws InterruptedException
   *
   */
  @Test
  public void getApexDFSRootDirRelPathVarAndImpersonation() throws IOException, InterruptedException
  {
    final Configuration conf = new Configuration(false);
    conf.set(StramClientUtils.APEX_APP_DFS_ROOT_DIR, "apex/%USER_NAME%/xyz");
    conf.setBoolean(StramUserLogin.DT_APP_PATH_IMPERSONATED, true);

    final FileSystem fs = FileSystem.newInstance(conf);
    UserGroupInformation testUser = UserGroupInformation.createUserForTesting("testUser1", new String[]{""});
    UserGroupInformation.setLoginUser(testUser);
    UserGroupInformation doAsUser = UserGroupInformation.createUserForTesting("testUser2", new String[]{""});

    doAsUser.doAs(new PrivilegedExceptionAction<Void>()
    {
      @Override
      public Void run() throws Exception
      {
        Path path = StramClientUtils.getApexDFSRootDir(fs, conf);
        Assert.assertEquals("file:/user/testUser2/apex/testUser2/xyz", path.toString());
        return null;
      }
    });
  }
}
