/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Arrays;
<<<<<<< HEAD
=======
import java.util.List;
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
import java.util.UUID;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException;
import org.apache.hadoop.fs.azurebfs.extensions.MockSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.MOCK_SASTOKENPROVIDER_FAIL_INIT;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.MOCK_SASTOKENPROVIDER_RETURN_EMPTY_SAS_TOKEN;
import static org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers.aclEntry;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test Perform Authorization Check operation
 */
public class ITestAzureBlobFileSystemAuthorization extends AbstractAbfsIntegrationTest {

<<<<<<< HEAD
  private static final String TEST_AUTHZ_CLASS = "org.apache.hadoop.fs.azurebfs.extensions.MockSASTokenProvider";
  private static final String TEST_ERR_AUTHZ_CLASS = "org.apache.hadoop.fs.azurebfs.extensions.MockErrorSASTokenProvider";
=======
  private static final Path TEST_READ_ONLY_FILE_PATH_0 = new Path(TEST_READ_ONLY_FILE_0);
  private static final Path TEST_READ_ONLY_FOLDER_PATH = new Path(TEST_READ_ONLY_FOLDER);
  private static final Path TEST_WRITE_ONLY_FILE_PATH_0 = new Path(TEST_WRITE_ONLY_FILE_0);
  private static final Path TEST_WRITE_ONLY_FILE_PATH_1 = new Path(TEST_WRITE_ONLY_FILE_1);
  private static final Path TEST_READ_WRITE_FILE_PATH_0 = new Path(TEST_READ_WRITE_FILE_0);
  private static final Path TEST_READ_WRITE_FILE_PATH_1 = new Path(TEST_READ_WRITE_FILE_1);
  private static final Path TEST_WRITE_ONLY_FOLDER_PATH = new Path(TEST_WRITE_ONLY_FOLDER);
  private static final Path TEST_WRITE_THEN_READ_ONLY_PATH = new Path(TEST_WRITE_THEN_READ_ONLY);
  private static final String TEST_AUTHZ_CLASS = "org.apache.hadoop.fs.azurebfs.extensions.MockAbfsAuthorizer";
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  private static final String TEST_USER = UUID.randomUUID().toString();
  private static final String TEST_GROUP = UUID.randomUUID().toString();
  private static final String BAR = UUID.randomUUID().toString();

  public ITestAzureBlobFileSystemAuthorization() throws Exception {
    // The mock SAS token provider relies on the account key to generate SAS.
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
  }

  @Override
  public void setup() throws Exception {
    boolean isHNSEnabled = this.getConfiguration().getBoolean(
        TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    Assume.assumeTrue(isHNSEnabled);
    loadConfiguredFileSystem();
    this.getConfiguration().set(ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, TEST_AUTHZ_CLASS);
    this.getConfiguration().set(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
    super.setup();
  }

  @Test
  public void testSASTokenProviderInitializeException() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    final AzureBlobFileSystem testFs = new AzureBlobFileSystem();
    Configuration testConfig = this.getConfiguration().getRawConfiguration();
    testConfig.set(ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, TEST_ERR_AUTHZ_CLASS);
    testConfig.set(MOCK_SASTOKENPROVIDER_FAIL_INIT, "true");

    intercept(TokenAccessProviderException.class,
        ()-> {
          testFs.initialize(fs.getUri(), this.getConfiguration().getRawConfiguration());
        });
  }

  @Test
  public void testSASTokenProviderEmptySASToken() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    final AzureBlobFileSystem testFs = new AzureBlobFileSystem();
    Configuration testConfig = this.getConfiguration().getRawConfiguration();
    testConfig.set(ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, TEST_ERR_AUTHZ_CLASS);
    testConfig.set(MOCK_SASTOKENPROVIDER_RETURN_EMPTY_SAS_TOKEN, "true");

    testFs.initialize(fs.getUri(),
        this.getConfiguration().getRawConfiguration());
    intercept(SASTokenProviderException.class,
        () -> {
          testFs.create(new org.apache.hadoop.fs.Path("/testFile"));
        });
  }

  @Test
  public void testSASTokenProviderNullSASToken() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    final AzureBlobFileSystem testFs = new AzureBlobFileSystem();
    Configuration testConfig = this.getConfiguration().getRawConfiguration();
    testConfig.set(ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE, TEST_ERR_AUTHZ_CLASS);

    testFs.initialize(fs.getUri(), this.getConfiguration().getRawConfiguration());
    intercept(SASTokenProviderException.class,
        ()-> {
          testFs.create(new org.apache.hadoop.fs.Path("/testFile"));
        });
  }

  @Test
  public void testOpenFileWithInvalidPath() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    intercept(IllegalArgumentException.class,
        ()-> {
          fs.open(new Path("")).close();
    });
  }

  @Test
  public void testOpenFileAuthorized() throws Exception {
    runTest(FileSystemOperations.Open, false);
  }

  @Test
  public void testOpenFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.Open, true);
  }

  @Test
  public void testCreateFileAuthorized() throws Exception {
    runTest(FileSystemOperations.CreatePath, false);
  }

  @Test
  public void testCreateFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.CreatePath, true);
  }

  @Test
  public void testAppendFileAuthorized() throws Exception {
    runTest(FileSystemOperations.Append, false);
  }

  @Test
  public void testAppendFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.Append, true);
  }

  @Test
  public void testRenameAuthorized() throws Exception {
    runTest(FileSystemOperations.RenamePath, false);
  }

  @Test
  public void testRenameUnauthorized() throws Exception {
    runTest(FileSystemOperations.RenamePath, true);
  }

  @Test
  public void testDeleteFileAuthorized() throws Exception {
    runTest(FileSystemOperations.DeletePath, false);
  }

  @Test
  public void testDeleteFileUnauthorized() throws Exception {
    runTest(FileSystemOperations.DeletePath, true);
  }

  @Test
  public void testListStatusAuthorized() throws Exception {
    runTest(FileSystemOperations.ListPaths, false);
  }

  @Test
  public void testListStatusUnauthorized() throws Exception {
    runTest(FileSystemOperations.ListPaths, true);
  }

  @Test
  public void testMkDirsAuthorized() throws Exception {
    runTest(FileSystemOperations.Mkdir, false);
  }

  @Test
  public void testMkDirsUnauthorized() throws Exception {
    runTest(FileSystemOperations.Mkdir, true);
  }

  @Test
  public void testGetFileStatusAuthorized() throws Exception {
    runTest(FileSystemOperations.GetPathStatus, false);
  }

  @Test
  public void testGetFileStatusUnauthorized() throws Exception {
<<<<<<< HEAD
    runTest(FileSystemOperations.GetPathStatus, true);
=======
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    intercept(AbfsAuthorizationException.class,
        ()-> {
          fs.getFileStatus(TEST_WRITE_ONLY_FILE_PATH_0);
    });
  }

  @Test
  public void testSetOwnerAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    fs.setOwner(TEST_WRITE_ONLY_FILE_PATH_0, TEST_USER, TEST_GROUP);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  @Test
  public void testSetOwnerUnauthorized() throws Exception {
<<<<<<< HEAD
    Assume.assumeTrue(getIsNamespaceEnabled(getFileSystem()));
    runTest(FileSystemOperations.SetOwner, true);
=======
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    intercept(AbfsAuthorizationException.class,
        ()-> {
          fs.setOwner(TEST_WRITE_THEN_READ_ONLY_PATH, TEST_USER, TEST_GROUP);
    });
  }

  @Test
  public void testSetPermissionAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    fs.setPermission(TEST_WRITE_ONLY_FILE_PATH_0, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  @Test
  public void testSetPermissionUnauthorized() throws Exception {
<<<<<<< HEAD
    Assume.assumeTrue(getIsNamespaceEnabled(getFileSystem()));
    runTest(FileSystemOperations.SetPermissions, true);
=======
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    intercept(AbfsAuthorizationException.class,
        ()-> {
          fs.setPermission(TEST_WRITE_THEN_READ_ONLY_PATH, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    });
  }

  @Test
  public void testModifyAclEntriesAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.modifyAclEntries(TEST_WRITE_ONLY_FILE_PATH_0, aclSpec);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  @Test
  public void testModifyAclEntriesUnauthorized() throws Exception {
<<<<<<< HEAD
    Assume.assumeTrue(getIsNamespaceEnabled(getFileSystem()));
    runTest(FileSystemOperations.ModifyAclEntries, true);
=======
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class,
        ()-> {
          fs.modifyAclEntries(TEST_WRITE_THEN_READ_ONLY_PATH, aclSpec);
    });
  }

  @Test
  public void testRemoveAclEntriesAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.removeAclEntries(TEST_WRITE_ONLY_FILE_PATH_0, aclSpec);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  @Test
  public void testRemoveAclEntriesUnauthorized() throws Exception {
<<<<<<< HEAD
    Assume.assumeTrue(getIsNamespaceEnabled(getFileSystem()));
    runTest(FileSystemOperations.RemoveAclEntries, true);
=======
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class,
        ()-> {
          fs.removeAclEntries(TEST_WRITE_THEN_READ_ONLY_PATH, aclSpec);
    });
  }

  @Test
  public void testRemoveDefaultAclAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    fs.removeDefaultAcl(TEST_WRITE_ONLY_FILE_PATH_0);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  @Test
  public void testRemoveDefaultAclUnauthorized() throws Exception {
<<<<<<< HEAD
    Assume.assumeTrue(getIsNamespaceEnabled(getFileSystem()));
    runTest(FileSystemOperations.RemoveDefaultAcl, true);
=======
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    intercept(AbfsAuthorizationException.class,
        ()-> {
          fs.removeDefaultAcl(TEST_WRITE_THEN_READ_ONLY_PATH);
    });
  }

  @Test
  public void testRemoveAclAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    fs.removeAcl(TEST_WRITE_ONLY_FILE_PATH_0);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  @Test
  public void testRemoveAclUnauthorized() throws Exception {
<<<<<<< HEAD
    Assume.assumeTrue(getIsNamespaceEnabled(getFileSystem()));
    runTest(FileSystemOperations.RemoveAcl, true);
=======
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    intercept(AbfsAuthorizationException.class,
        ()-> {
          fs.removeAcl(TEST_WRITE_THEN_READ_ONLY_PATH);
    });
  }

  @Test
  public void testSetAclAuthorized() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.setAcl(TEST_WRITE_ONLY_FILE_PATH_0, aclSpec);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  @Test
  public void testSetAclUnauthorized() throws Exception {
<<<<<<< HEAD
    Assume.assumeTrue(getIsNamespaceEnabled(getFileSystem()));
    runTest(FileSystemOperations.SetAcl, true);
=======
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class,
        ()-> {
          fs.setAcl(TEST_WRITE_THEN_READ_ONLY_PATH, aclSpec);
    });
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  @Test
  public void testGetAclStatusAuthorized() throws Exception {
<<<<<<< HEAD
    Assume.assumeTrue(getIsNamespaceEnabled(getFileSystem()));
    runTest(FileSystemOperations.GetAcl, false);
=======
    final AzureBlobFileSystem fs = getFileSystem();
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_THEN_READ_ONLY_PATH).close();
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    fs.getAclStatus(TEST_WRITE_THEN_READ_ONLY_PATH);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  @Test
  public void testGetAclStatusUnauthorized() throws Exception {
    Assume.assumeTrue(getIsNamespaceEnabled(getFileSystem()));
    runTest(FileSystemOperations.GetAcl, true);
  }


  private void runTest(FileSystemOperations testOp,
      boolean expectAbfsAuthorizationException) throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
<<<<<<< HEAD

    Path reqPath = new Path("requestPath"
        + UUID.randomUUID().toString()
        + (expectAbfsAuthorizationException ? "unauthorized":""));

    getMockSASTokenProvider(fs).setSkipAuthorizationForTestSetup(true);
    if ((testOp != FileSystemOperations.CreatePath)
        && (testOp != FileSystemOperations.Mkdir)) {
      fs.create(reqPath).close();
      fs.getFileStatus(reqPath);
    }
    getMockSASTokenProvider(fs).setSkipAuthorizationForTestSetup(false);

    // Test Operation
    if (expectAbfsAuthorizationException) {
      intercept(SASTokenProviderException.class, () -> {
        executeOp(reqPath, fs, testOp);
      });
    } else {
      executeOp(reqPath, fs, testOp);
    }
  }

  private void executeOp(Path reqPath, AzureBlobFileSystem fs,
      FileSystemOperations op) throws IOException, IOException {


    switch (op) {
    case ListPaths:
      fs.listStatus(reqPath);
      break;
    case CreatePath:
      fs.create(reqPath);
      break;
    case RenamePath:
      fs.rename(reqPath,
          new Path("renameDest" + UUID.randomUUID().toString()));
      break;
    case GetAcl:
      fs.getAclStatus(reqPath);
      break;
    case GetPathStatus:
      fs.getFileStatus(reqPath);
      break;
    case SetAcl:
      fs.setAcl(reqPath, Arrays
          .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL)));
      break;
    case SetOwner:
      fs.setOwner(reqPath, TEST_USER, TEST_GROUP);
      break;
    case SetPermissions:
      fs.setPermission(reqPath,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      break;
    case Append:
      fs.append(reqPath);
      break;
    case ReadFile:
      fs.open(reqPath);
      break;
    case Open:
      fs.open(reqPath);
      break;
    case DeletePath:
      fs.delete(reqPath, false);
      break;
    case Mkdir:
      fs.mkdirs(reqPath,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      break;
    case RemoveAclEntries:
      fs.removeAclEntries(reqPath, Arrays
          .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL)));
      break;
    case ModifyAclEntries:
      fs.modifyAclEntries(reqPath, Arrays
          .asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL)));
      break;
    case RemoveAcl:
      fs.removeAcl(reqPath);
      break;
    case RemoveDefaultAcl:
      fs.removeDefaultAcl(reqPath);
      break;
    default:
      throw new IllegalStateException("Unexpected value: " + op);
    }
  }

  private MockSASTokenProvider getMockSASTokenProvider(AzureBlobFileSystem fs)
      throws Exception {
    return ((MockSASTokenProvider) fs.getAbfsStore().getClient().getSasTokenProvider());
  }

  enum FileSystemOperations {
    None, ListPaths, CreatePath, RenamePath, GetAcl, GetPathStatus, SetAcl,
    SetOwner, SetPermissions, Append, ReadFile, DeletePath, Mkdir,
    RemoveAclEntries, RemoveDefaultAcl, RemoveAcl, ModifyAclEntries,
    Open
=======
    assumeTrue("This test case only runs when namespace is enabled", fs.getIsNamespaceEnabled());
    fs.create(TEST_WRITE_ONLY_FILE_PATH_0).close();
    List<AclEntry> aclSpec = Arrays.asList(aclEntry(ACCESS, GROUP, BAR, FsAction.ALL));
    intercept(AbfsAuthorizationException.class,
        ()-> {
          fs.getAclStatus(TEST_WRITE_ONLY_FILE_PATH_0);
    });
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }
}
