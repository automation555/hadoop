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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
<<<<<<< HEAD
import java.lang.reflect.InvocationTargetException;
import java.io.UnsupportedEncodingException;
=======
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
<<<<<<< HEAD
=======
import java.text.ParseException;
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
<<<<<<< HEAD
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
=======

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConcurrentWriteOperationDetectedException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.FileSystemOperationUnhandledException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidFileSystemPropertyException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultEntrySchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TrileanConversionException;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
<<<<<<< HEAD
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformer;
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformerInterface;
=======
import org.apache.hadoop.fs.azurebfs.oauth2.IdentityTransformer;
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
import org.apache.hadoop.fs.azurebfs.services.AbfsAclHelper;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientContextBuilder;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamStatisticsImpl;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamStatisticsImpl;
import org.apache.hadoop.fs.azurebfs.services.AbfsPermission;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;
import org.apache.hadoop.fs.azurebfs.services.AbfsLease;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.apache.hadoop.fs.azurebfs.services.AbfsPerfTracker;
import org.apache.hadoop.fs.azurebfs.services.AbfsPerfInfo;
import org.apache.hadoop.fs.azurebfs.services.ListingSupport;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.CRC64;
<<<<<<< HEAD
import org.apache.hadoop.fs.azurebfs.utils.DateTimeUtils;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
=======
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.client.utils.URIBuilder;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_EQUALS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_FORWARD_SLASH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_HYPHEN;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_PLUS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_STAR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.CHAR_UNDERSCORE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.SINGLE_WHITE_SPACE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TOKEN_VERSION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_ABFS_ENDPOINT;
<<<<<<< HEAD
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_BUFFERED_PREAD_DISABLE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_IDENTITY_TRANSFORM_CLASS;
=======
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

/**
 * Provides the bridging logic between Hadoop's abstract filesystem and Azure Storage.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AzureBlobFileSystemStore implements Closeable, ListingSupport {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobFileSystemStore.class);

  private AbfsClient client;
  private URI uri;
  private String userName;
  private String primaryUserGroup;
<<<<<<< HEAD
  private static final String TOKEN_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'";
  private static final String XMS_PROPERTIES_ENCODING = "ISO-8859-1";
  private static final int GET_SET_AGGREGATE_COUNT = 2;

  private final Map<AbfsLease, Object> leaseRefs;

  private final AbfsConfiguration abfsConfiguration;
  private final Set<String> azureAtomicRenameDirSet;
  private Set<String> azureInfiniteLeaseDirSet;
  private Trilean isNamespaceEnabled;
  private final AuthType authType;
  private final UserGroupInformation userGroupInformation;
  private final IdentityTransformerInterface identityTransformer;
  private final AbfsPerfTracker abfsPerfTracker;
  private final AbfsCounters abfsCounters;

  /**
   * The set of directories where we should store files as append blobs.
   */
  private Set<String> appendBlobDirSet;

  public AzureBlobFileSystemStore(URI uri, boolean isSecureScheme,
                                  Configuration configuration,
                                  AbfsCounters abfsCounters) throws IOException {
=======
  private static final String DATE_TIME_PATTERN = "E, dd MMM yyyy HH:mm:ss z";
  private static final String TOKEN_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'";
  private static final String XMS_PROPERTIES_ENCODING = "ISO-8859-1";
  private static final int LIST_MAX_RESULTS = 500;

  private final AbfsConfiguration abfsConfiguration;
  private final Set<String> azureAtomicRenameDirSet;
  private boolean isNamespaceEnabledSet;
  private boolean isNamespaceEnabled;
  private final AuthType authType;
  private final UserGroupInformation userGroupInformation;
  private final IdentityTransformer identityTransformer;

  public AzureBlobFileSystemStore(URI uri, boolean isSecureScheme, Configuration configuration)
          throws IOException {
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
    this.uri = uri;
    String[] authorityParts = authorityParts(uri);
    final String fileSystemName = authorityParts[0];
    final String accountName = authorityParts[1];

    leaseRefs = Collections.synchronizedMap(new WeakHashMap<>());

    try {
      this.abfsConfiguration = new AbfsConfiguration(configuration, accountName);
    } catch (IllegalAccessException exception) {
      throw new FileSystemOperationUnhandledException(exception);
    }
<<<<<<< HEAD

    LOG.trace("AbfsConfiguration init complete");

    this.isNamespaceEnabled = abfsConfiguration.getIsNamespaceEnabledAccount();

    this.userGroupInformation = UserGroupInformation.getCurrentUser();
    this.userName = userGroupInformation.getShortUserName();
    LOG.trace("UGI init complete");
=======
    this.userGroupInformation = UserGroupInformation.getCurrentUser();
    this.userName = userGroupInformation.getShortUserName();
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
    if (!abfsConfiguration.getSkipUserGroupMetadataDuringInitialization()) {
      try {
        this.primaryUserGroup = userGroupInformation.getPrimaryGroupName();
      } catch (IOException ex) {
        LOG.error("Failed to get primary group for {}, using user name as primary group name", userName);
        this.primaryUserGroup = userName;
      }
    } else {
      //Provide a default group name
      this.primaryUserGroup = userName;
    }
    LOG.trace("primaryUserGroup is {}", this.primaryUserGroup);

    this.azureAtomicRenameDirSet = new HashSet<>(Arrays.asList(
        abfsConfiguration.getAzureAtomicRenameDirs().split(AbfsHttpConstants.COMMA)));
<<<<<<< HEAD
    updateInfiniteLeaseDirs();
    this.authType = abfsConfiguration.getAuthType(accountName);
    boolean usingOauth = (authType == AuthType.OAuth);
    boolean useHttps = (usingOauth || abfsConfiguration.isHttpsAlwaysUsed()) ? true : isSecureScheme;
    this.abfsPerfTracker = new AbfsPerfTracker(fileSystemName, accountName, this.abfsConfiguration);
    this.abfsCounters = abfsCounters;
    initializeClient(uri, fileSystemName, accountName, useHttps);
    final Class<? extends IdentityTransformerInterface> identityTransformerClass =
        configuration.getClass(FS_AZURE_IDENTITY_TRANSFORM_CLASS, IdentityTransformer.class,
            IdentityTransformerInterface.class);
    try {
      this.identityTransformer =
          identityTransformerClass.getConstructor(Configuration.class).newInstance(configuration);
    } catch (IllegalAccessException | InstantiationException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException e) {
      throw new IOException(e);
    }
    LOG.trace("IdentityTransformer init complete");

    // Extract the directories that should contain append blobs
    String appendBlobDirs = abfsConfiguration.getAppendBlobDirs();
    if (appendBlobDirs.trim().isEmpty()) {
      this.appendBlobDirSet = new HashSet<String>();
    } else {
      this.appendBlobDirSet = new HashSet<>(Arrays.asList(
          abfsConfiguration.getAppendBlobDirs().split(AbfsHttpConstants.COMMA)));
    }
  }

  /**
   * Checks if the given key in Azure Storage should be stored as a page
   * blob instead of block blob.
   */
  public boolean isAppendBlobKey(String key) {
    return isKeyForDirectorySet(key, appendBlobDirSet);
  }

  /**
   * @return local user name.
   * */
  public String getUser() {
    return this.userName;
  }

  /**
=======
    this.authType = abfsConfiguration.getAuthType(accountName);
    boolean usingOauth = (authType == AuthType.OAuth);
    boolean useHttps = (usingOauth || abfsConfiguration.isHttpsAlwaysUsed()) ? true : isSecureScheme;
    initializeClient(uri, fileSystemName, accountName, useHttps);
    this.identityTransformer = new IdentityTransformer(abfsConfiguration.getRawConfiguration());
  }

  /**
   * @return local user name.
   * */
  public String getUser() {
    return this.userName;
  }

  /**
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  * @return primary group that user belongs to.
  * */
  public String getPrimaryGroup() {
    return this.primaryUserGroup;
<<<<<<< HEAD
  }

  @Override
  public void close() throws IOException {
    List<ListenableFuture<?>> futures = new ArrayList<>();
    for (AbfsLease lease : leaseRefs.keySet()) {
      if (lease == null) {
        continue;
      }
      ListenableFuture<?> future = client.submit(() -> lease.free());
      futures.add(future);
    }
    try {
      Futures.allAsList(futures).get();
    } catch (InterruptedException e) {
      LOG.error("Interrupted freeing leases", e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOG.error("Error freeing leases", e);
    } finally {
      IOUtils.cleanupWithLogger(LOG, client);
    }
  }

  byte[] encodeAttribute(String value) throws UnsupportedEncodingException {
    return value.getBytes(XMS_PROPERTIES_ENCODING);
  }

  String decodeAttribute(byte[] value) throws UnsupportedEncodingException {
    return new String(value, XMS_PROPERTIES_ENCODING);
=======
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  private String[] authorityParts(URI uri) throws InvalidUriAuthorityException, InvalidUriException {
    final String authority = uri.getRawAuthority();
    if (null == authority) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    if (!authority.contains(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER)) {
      throw new InvalidUriAuthorityException(uri.toString());
    }

    final String[] authorityParts = authority.split(AbfsHttpConstants.AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER, 2);

    if (authorityParts.length < 2 || authorityParts[0] != null
        && authorityParts[0].isEmpty()) {
      final String errMsg = String
              .format("'%s' has a malformed authority, expected container name. "
                      + "Authority takes the form "
                      + FileSystemUriSchemes.ABFS_SCHEME + "://[<container name>@]<account name>",
                      uri.toString());
      throw new InvalidUriException(errMsg);
    }
    return authorityParts;
  }

<<<<<<< HEAD
  public boolean getIsNamespaceEnabled(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try {
      return this.isNamespaceEnabled.toBoolean();
    } catch (TrileanConversionException e) {
      LOG.debug("isNamespaceEnabled is UNKNOWN; fall back and determine through"
          + " getAcl server call", e);
    }

    LOG.debug("Get root ACL status");
    try (AbfsPerfInfo perfInfo = startTracking("getIsNamespaceEnabled",
        "getAclStatus")) {
      AbfsRestOperation op = client
          .getAclStatus(AbfsHttpConstants.ROOT_PATH, tracingContext);
      perfInfo.registerResult(op.getResult());
      isNamespaceEnabled = Trilean.getTrilean(true);
      perfInfo.registerSuccess(true);
    } catch (AbfsRestOperationException ex) {
      // Get ACL status is a HEAD request, its response doesn't contain
      // errorCode
      // So can only rely on its status code to determine its account type.
      if (HttpURLConnection.HTTP_BAD_REQUEST != ex.getStatusCode()) {
        throw ex;
      }

      isNamespaceEnabled = Trilean.getTrilean(false);
=======
  public boolean getIsNamespaceEnabled() throws AzureBlobFileSystemException {
    if (!isNamespaceEnabledSet) {
      LOG.debug("Get root ACL status");
      try {
        client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + AbfsHttpConstants.ROOT_PATH);
        isNamespaceEnabled = true;
      } catch (AbfsRestOperationException ex) {
        // Get ACL status is a HEAD request, its response doesn't contain errorCode
        // So can only rely on its status code to determine its account type.
        if (HttpURLConnection.HTTP_BAD_REQUEST != ex.getStatusCode()) {
          throw ex;
        }
        isNamespaceEnabled = false;
      }
      isNamespaceEnabledSet = true;
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
    }

    return isNamespaceEnabled.toBoolean();
  }

  @VisibleForTesting
  URIBuilder getURIBuilder(final String hostName, boolean isSecure) {
    String scheme = isSecure ? FileSystemUriSchemes.HTTPS_SCHEME : FileSystemUriSchemes.HTTP_SCHEME;

    final URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme(scheme);

    // For testing purposes, an IP address and port may be provided to override
    // the host specified in the FileSystem URI.  Also note that the format of
    // the Azure Storage Service URI changes from
    // http[s]://[account][domain-suffix]/[filesystem] to
    // http[s]://[ip]:[port]/[account]/[filesystem].
    String endPoint = abfsConfiguration.get(AZURE_ABFS_ENDPOINT);
    if (endPoint == null || !endPoint.contains(AbfsHttpConstants.COLON)) {
      uriBuilder.setHost(hostName);
      return uriBuilder;
    }

    // Split ip and port
    String[] data = endPoint.split(AbfsHttpConstants.COLON);
    if (data.length != 2) {
      throw new RuntimeException(String.format("ABFS endpoint is not set correctly : %s, "
              + "Do not specify scheme when using {IP}:{PORT}", endPoint));
    }
    uriBuilder.setHost(data[0].trim());
    uriBuilder.setPort(Integer.parseInt(data[1].trim()));
    uriBuilder.setPath("/" + UriUtils.extractAccountNameFromHostName(hostName));

    return uriBuilder;
  }

  public AbfsConfiguration getAbfsConfiguration() {
    return this.abfsConfiguration;
  }

  public Hashtable<String, String> getFilesystemProperties(
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("getFilesystemProperties",
            "getFilesystemProperties")) {
      LOG.debug("getFilesystemProperties for filesystem: {}",
              client.getFileSystem());

      final Hashtable<String, String> parsedXmsProperties;

      final AbfsRestOperation op = client
          .getFilesystemProperties(tracingContext);
      perfInfo.registerResult(op.getResult());

      final String xMsProperties = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_PROPERTIES);

      parsedXmsProperties = parseCommaSeparatedXmsProperties(xMsProperties);
      perfInfo.registerSuccess(true);

      return parsedXmsProperties;
    }
  }

  public void setFilesystemProperties(
      final Hashtable<String, String> properties, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (properties == null || properties.isEmpty()) {
      LOG.trace("setFilesystemProperties no properties present");
      return;
    }

    LOG.debug("setFilesystemProperties for filesystem: {} with properties: {}",
            client.getFileSystem(),
            properties);

    try (AbfsPerfInfo perfInfo = startTracking("setFilesystemProperties",
            "setFilesystemProperties")) {
      final String commaSeparatedProperties;
      try {
        commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
      } catch (CharacterCodingException ex) {
        throw new InvalidAbfsRestOperationException(ex);
      }

      final AbfsRestOperation op = client
          .setFilesystemProperties(commaSeparatedProperties, tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

<<<<<<< HEAD
  public Hashtable<String, String> getPathStatus(final Path path,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("getPathStatus", "getPathStatus")){
      LOG.debug("getPathStatus for filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      final Hashtable<String, String> parsedXmsProperties;
      final AbfsRestOperation op = client
          .getPathStatus(getRelativePath(path), true, tracingContext);
      perfInfo.registerResult(op.getResult());

      final String xMsProperties = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_PROPERTIES);
=======
  public Hashtable<String, String> getPathStatus(final Path path) throws AzureBlobFileSystemException {
    LOG.debug("getPathStatus for filesystem: {} path: {}",
            client.getFileSystem(),
           path);

    final Hashtable<String, String> parsedXmsProperties;
    final AbfsRestOperation op = client.getPathStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

      parsedXmsProperties = parseCommaSeparatedXmsProperties(xMsProperties);

      perfInfo.registerSuccess(true);

      return parsedXmsProperties;
    }
  }

  public void setPathProperties(final Path path,
      final Hashtable<String, String> properties, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("setPathProperties", "setPathProperties")){
      LOG.debug("setFilesystemProperties for filesystem: {} path: {} with properties: {}",
              client.getFileSystem(),
              path,
              properties);

      final String commaSeparatedProperties;
      try {
        commaSeparatedProperties = convertXmsPropertiesToCommaSeparatedString(properties);
      } catch (CharacterCodingException ex) {
        throw new InvalidAbfsRestOperationException(ex);
      }
      final AbfsRestOperation op = client
          .setPathProperties(getRelativePath(path), commaSeparatedProperties,
              tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public void createFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("createFilesystem", "createFilesystem")){
      LOG.debug("createFilesystem for filesystem: {}",
              client.getFileSystem());

      final AbfsRestOperation op = client.createFilesystem(tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public void deleteFilesystem(TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("deleteFilesystem", "deleteFilesystem")) {
      LOG.debug("deleteFilesystem for filesystem: {}",
              client.getFileSystem());

      final AbfsRestOperation op = client.deleteFilesystem(tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public OutputStream createFile(final Path path,
      final FileSystem.Statistics statistics, final boolean overwrite,
      final FsPermission permission, final FsPermission umask,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("createFile", "createPath")) {
      boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
      LOG.debug("createFile filesystem: {} path: {} overwrite: {} permission: {} umask: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              overwrite,
              permission,
              umask,
              isNamespaceEnabled);

      String relativePath = getRelativePath(path);
      boolean isAppendBlob = false;
      if (isAppendBlobKey(path.toString())) {
        isAppendBlob = true;
      }

      // if "fs.azure.enable.conditional.create.overwrite" is enabled and
      // is a create request with overwrite=true, create will follow different
      // flow.
      boolean triggerConditionalCreateOverwrite = false;
      if (overwrite
          && abfsConfiguration.isConditionalCreateOverwriteEnabled()) {
        triggerConditionalCreateOverwrite = true;
      }

      AbfsRestOperation op;
      if (triggerConditionalCreateOverwrite) {
        op = conditionalCreateOverwriteFile(relativePath,
            statistics,
            isNamespaceEnabled ? getOctalNotation(permission) : null,
            isNamespaceEnabled ? getOctalNotation(umask) : null,
            isAppendBlob,
            tracingContext
        );

      } else {
        op = client.createPath(relativePath, true,
            overwrite,
            isNamespaceEnabled ? getOctalNotation(permission) : null,
            isNamespaceEnabled ? getOctalNotation(umask) : null,
            isAppendBlob,
            null,
            tracingContext);

      }
      perfInfo.registerResult(op.getResult()).registerSuccess(true);

      AbfsLease lease = maybeCreateLease(relativePath, tracingContext);

      return new AbfsOutputStream(
          client,
          statistics,
          relativePath,
          0,
          populateAbfsOutputStreamContext(isAppendBlob, lease),
          tracingContext);
    }
  }

  /**
   * Conditional create overwrite flow ensures that create overwrites is done
   * only if there is match for eTag of existing file.
   * @param relativePath
   * @param statistics
   * @param permission
   * @param umask
   * @param isAppendBlob
   * @return
   * @throws AzureBlobFileSystemException
   */
  private AbfsRestOperation conditionalCreateOverwriteFile(final String relativePath,
      final FileSystem.Statistics statistics,
      final String permission,
      final String umask,
      final boolean isAppendBlob,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    AbfsRestOperation op;

    try {
      // Trigger a create with overwrite=false first so that eTag fetch can be
      // avoided for cases when no pre-existing file is present (major portion
      // of create file traffic falls into the case of no pre-existing file).
      op = client.createPath(relativePath, true, false, permission, umask,
          isAppendBlob, null, tracingContext);

    } catch (AbfsRestOperationException e) {
      if (e.getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
        // File pre-exists, fetch eTag
        try {
          op = client.getPathStatus(relativePath, false, tracingContext);
        } catch (AbfsRestOperationException ex) {
          if (ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
            // Is a parallel access case, as file which was found to be
            // present went missing by this request.
            throw new ConcurrentWriteOperationDetectedException(
                "Parallel access to the create path detected. Failing request "
                    + "to honor single writer semantics");
          } else {
            throw ex;
          }
        }

        String eTag = op.getResult()
            .getResponseHeader(HttpHeaderConfigurations.ETAG);

        try {
          // overwrite only if eTag matches with the file properties fetched befpre
          op = client.createPath(relativePath, true, true, permission, umask,
              isAppendBlob, eTag, tracingContext);
        } catch (AbfsRestOperationException ex) {
          if (ex.getStatusCode() == HttpURLConnection.HTTP_PRECON_FAILED) {
            // Is a parallel access case, as file with eTag was just queried
            // and precondition failure can happen only when another file with
            // different etag got created.
            throw new ConcurrentWriteOperationDetectedException(
                "Parallel access to the create path detected. Failing request "
                    + "to honor single writer semantics");
          } else {
            throw ex;
          }
        }
      } else {
        throw e;
      }
    }

    return op;
  }

  private AbfsOutputStreamContext populateAbfsOutputStreamContext(boolean isAppendBlob,
      AbfsLease lease) {
    int bufferSize = abfsConfiguration.getWriteBufferSize();
    if (isAppendBlob && bufferSize > FileSystemConfigurations.APPENDBLOB_MAX_WRITE_BUFFER_SIZE) {
      bufferSize = FileSystemConfigurations.APPENDBLOB_MAX_WRITE_BUFFER_SIZE;
    }
    return new AbfsOutputStreamContext(abfsConfiguration.getSasTokenRenewPeriodForStreamsInSeconds())
            .withWriteBufferSize(bufferSize)
            .enableFlush(abfsConfiguration.isFlushEnabled())
            .enableSmallWriteOptimization(abfsConfiguration.isSmallWriteOptimizationEnabled())
            .disableOutputStreamFlush(abfsConfiguration.isOutputStreamFlushDisabled())
            .withStreamStatistics(new AbfsOutputStreamStatisticsImpl())
            .withAppendBlob(isAppendBlob)
            .withWriteMaxConcurrentRequestCount(abfsConfiguration.getWriteMaxConcurrentRequestCount())
            .withMaxWriteRequestsToQueue(abfsConfiguration.getMaxWriteRequestsToQueue())
            .withLease(lease)
            .build();
  }

  public void createDirectory(final Path path, final FsPermission permission,
      final FsPermission umask, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("createDirectory", "createPath")) {
      boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
      LOG.debug("createDirectory filesystem: {} path: {} permission: {} umask: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              permission,
              umask,
              isNamespaceEnabled);

      boolean overwrite =
          !isNamespaceEnabled || abfsConfiguration.isEnabledMkdirOverwrite();
      final AbfsRestOperation op = client.createPath(getRelativePath(path),
          false, overwrite,
              isNamespaceEnabled ? getOctalNotation(permission) : null,
              isNamespaceEnabled ? getOctalNotation(umask) : null, false, null,
              tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public AbfsInputStream openFileForRead(final Path path,
      final FileSystem.Statistics statistics, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    return openFileForRead(path, Optional.empty(), statistics, tracingContext);
  }

  public AbfsInputStream openFileForRead(final Path path,
      final Optional<Configuration> options,
      final FileSystem.Statistics statistics, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("openFileForRead", "getPathStatus")) {
      LOG.debug("openFileForRead filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = client
          .getPathStatus(relativePath, false, tracingContext);
      perfInfo.registerResult(op.getResult());

<<<<<<< HEAD
      final String resourceType = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE);
      final long contentLength = Long.parseLong(op.getResult().getResponseHeader(HttpHeaderConfigurations.CONTENT_LENGTH));
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);
=======
    final AbfsRestOperation op = client.getPathStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

      if (parseIsDirectory(resourceType)) {
        throw new AbfsRestOperationException(
                AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                "openFileForRead must be used with files and not directories",
                null);
      }

      perfInfo.registerSuccess(true);

      // Add statistics for InputStream
      return new AbfsInputStream(client, statistics,
              relativePath, contentLength,
              populateAbfsInputStreamContext(options),
              eTag, tracingContext);
    }
  }

<<<<<<< HEAD
  private AbfsInputStreamContext populateAbfsInputStreamContext(
      Optional<Configuration> options) {
    boolean bufferedPreadDisabled = options
        .map(c -> c.getBoolean(FS_AZURE_BUFFERED_PREAD_DISABLE, false))
        .orElse(false);
    return new AbfsInputStreamContext(abfsConfiguration.getSasTokenRenewPeriodForStreamsInSeconds())
            .withReadBufferSize(abfsConfiguration.getReadBufferSize())
            .withReadAheadQueueDepth(abfsConfiguration.getReadAheadQueueDepth())
            .withTolerateOobAppends(abfsConfiguration.getTolerateOobAppends())
            .withReadSmallFilesCompletely(abfsConfiguration.readSmallFilesCompletely())
            .withOptimizeFooterRead(abfsConfiguration.optimizeFooterRead())
            .withStreamStatistics(new AbfsInputStreamStatisticsImpl())
            .withShouldReadBufferSizeAlways(
                abfsConfiguration.shouldReadBufferSizeAlways())
            .withReadAheadBlockSize(abfsConfiguration.getReadAheadBlockSize())
            .withBufferedPreadDisabled(bufferedPreadDisabled)
            .build();
=======
    // Add statistics for InputStream
    return new AbfsInputStream(client, statistics,
            AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), contentLength,
                abfsConfiguration.getReadBufferSize(), abfsConfiguration.getReadAheadQueueDepth(),
                abfsConfiguration.getTolerateOobAppends(), eTag);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  public OutputStream openFileForWrite(final Path path,
      final FileSystem.Statistics statistics, final boolean overwrite,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    try (AbfsPerfInfo perfInfo = startTracking("openFileForWrite", "getPathStatus")) {
      LOG.debug("openFileForWrite filesystem: {} path: {} overwrite: {}",
              client.getFileSystem(),
              path,
              overwrite);

<<<<<<< HEAD
      String relativePath = getRelativePath(path);
=======
    final AbfsRestOperation op = client.getPathStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

      final AbfsRestOperation op = client
          .getPathStatus(relativePath, false, tracingContext);
      perfInfo.registerResult(op.getResult());

      final String resourceType = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE);
      final Long contentLength = Long.valueOf(op.getResult().getResponseHeader(HttpHeaderConfigurations.CONTENT_LENGTH));

      if (parseIsDirectory(resourceType)) {
        throw new AbfsRestOperationException(
                AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                "openFileForRead must be used with files and not directories",
                null);
      }

      final long offset = overwrite ? 0 : contentLength;

      perfInfo.registerSuccess(true);

      boolean isAppendBlob = false;
      if (isAppendBlobKey(path.toString())) {
        isAppendBlob = true;
      }

      AbfsLease lease = maybeCreateLease(relativePath, tracingContext);

      return new AbfsOutputStream(
          client,
          statistics,
          relativePath,
          offset,
          populateAbfsOutputStreamContext(isAppendBlob, lease),
          tracingContext);
    }
  }

  /**
   * Break any current lease on an ABFS file.
   *
   * @param path file name
   * @param tracingContext TracingContext instance to track correlation IDs
   * @throws AzureBlobFileSystemException on any exception while breaking the lease
   */
  public void breakLease(final Path path, final TracingContext tracingContext) throws AzureBlobFileSystemException {
    LOG.debug("lease path: {}", path);

    client.breakLease(getRelativePath(path), tracingContext);
  }

  public void rename(final Path source, final Path destination, TracingContext tracingContext) throws
          AzureBlobFileSystemException {
    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue;

    if (isAtomicRenameKey(source.getName())) {
      LOG.warn("The atomic rename feature is not supported by the ABFS scheme; however rename,"
              +" create and delete operations are atomic if Namespace is enabled for your Azure Storage account.");
    }

    LOG.debug("renameAsync filesystem: {} source: {} destination: {}",
            client.getFileSystem(),
            source,
            destination);

    String continuation = null;

<<<<<<< HEAD
    String sourceRelativePath = getRelativePath(source);
    String destinationRelativePath = getRelativePath(destination);

    do {
      try (AbfsPerfInfo perfInfo = startTracking("rename", "renamePath")) {
        AbfsRestOperation op = client
            .renamePath(sourceRelativePath, destinationRelativePath,
                continuation, tracingContext);
        perfInfo.registerResult(op.getResult());
        continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();

        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);
=======
    do {
      AbfsRestOperation op = client.renamePath(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(source),
              AbfsHttpConstants.FORWARD_SLASH + getRelativePath(destination), continuation);
      continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);

    } while (continuation != null && !continuation.isEmpty());
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  public void delete(final Path path, final boolean recursive,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue = true;

    LOG.debug("delete filesystem: {} path: {} recursive: {}",
            client.getFileSystem(),
            path,
            String.valueOf(recursive));

    String continuation = null;

<<<<<<< HEAD
    String relativePath = getRelativePath(path);

    do {
      try (AbfsPerfInfo perfInfo = startTracking("delete", "deletePath")) {
        AbfsRestOperation op = client
            .deletePath(relativePath, recursive, continuation, tracingContext);
        perfInfo.registerResult(op.getResult());
        continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue = continuation != null && !continuation.isEmpty();

        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);
  }

  public FileStatus getFileStatus(final Path path,
      TracingContext tracingContext) throws IOException {
    try (AbfsPerfInfo perfInfo = startTracking("getFileStatus", "undetermined")) {
      boolean isNamespaceEnabled = getIsNamespaceEnabled(tracingContext);
      LOG.debug("getFileStatus filesystem: {} path: {} isNamespaceEnabled: {}",
              client.getFileSystem(),
              path,
              isNamespaceEnabled);

      final AbfsRestOperation op;
      if (path.isRoot()) {
        if (isNamespaceEnabled) {
          perfInfo.registerCallee("getAclStatus");
          op = client.getAclStatus(getRelativePath(path), tracingContext);
        } else {
          perfInfo.registerCallee("getFilesystemProperties");
          op = client.getFilesystemProperties(tracingContext);
        }
      } else {
        perfInfo.registerCallee("getPathStatus");
        op = client.getPathStatus(getRelativePath(path), false, tracingContext);
      }

      perfInfo.registerResult(op.getResult());
      final long blockSize = abfsConfiguration.getAzureBlockSize();
      final AbfsHttpOperation result = op.getResult();

      final String eTag = result.getResponseHeader(HttpHeaderConfigurations.ETAG);
      final String lastModified = result.getResponseHeader(HttpHeaderConfigurations.LAST_MODIFIED);
      final String permissions = result.getResponseHeader((HttpHeaderConfigurations.X_MS_PERMISSIONS));
      final boolean hasAcl = AbfsPermission.isExtendedAcl(permissions);
      final long contentLength;
      final boolean resourceIsDir;

      if (path.isRoot()) {
        contentLength = 0;
        resourceIsDir = true;
      } else {
        contentLength = parseContentLength(result.getResponseHeader(HttpHeaderConfigurations.CONTENT_LENGTH));
        resourceIsDir = parseIsDirectory(result.getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE));
      }

      final String transformedOwner = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_OWNER),
              true,
              userName);

      final String transformedGroup = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_GROUP),
              false,
              primaryUserGroup);

      perfInfo.registerSuccess(true);

      return new VersionedFileStatus(
              transformedOwner,
              transformedGroup,
              permissions == null ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
                      : AbfsPermission.valueOf(permissions),
              hasAcl,
              contentLength,
              resourceIsDir,
              1,
              blockSize,
              DateTimeUtils.parseLastModifiedTime(lastModified),
              path,
              eTag);
=======
    do {
      AbfsRestOperation op = client.deletePath(
          AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path), recursive, continuation);
      continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);

    } while (continuation != null && !continuation.isEmpty());
  }

  public FileStatus getFileStatus(final Path path) throws IOException {
    boolean isNamespaceEnabled = getIsNamespaceEnabled();
    LOG.debug("getFileStatus filesystem: {} path: {} isNamespaceEnabled: {}",
            client.getFileSystem(),
            path,
            isNamespaceEnabled);

    final AbfsRestOperation op;
    if (path.isRoot()) {
      op = isNamespaceEnabled
              ? client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + AbfsHttpConstants.ROOT_PATH)
              : client.getFilesystemProperties();
    } else {
      op = client.getPathStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path));
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
    }

    final long blockSize = abfsConfiguration.getAzureBlockSize();
    final AbfsHttpOperation result = op.getResult();

    final String eTag = result.getResponseHeader(HttpHeaderConfigurations.ETAG);
    final String lastModified = result.getResponseHeader(HttpHeaderConfigurations.LAST_MODIFIED);
    final String permissions = result.getResponseHeader((HttpHeaderConfigurations.X_MS_PERMISSIONS));
    final boolean hasAcl = AbfsPermission.isExtendedAcl(permissions);
    final long contentLength;
    final boolean resourceIsDir;

    if (path.isRoot()) {
      contentLength = 0;
      resourceIsDir = true;
    } else {
      contentLength = parseContentLength(result.getResponseHeader(HttpHeaderConfigurations.CONTENT_LENGTH));
      resourceIsDir = parseIsDirectory(result.getResponseHeader(HttpHeaderConfigurations.X_MS_RESOURCE_TYPE));
    }

    final String transformedOwner = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_OWNER),
              true,
              userName);

    final String transformedGroup = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_GROUP),
              false,
              primaryUserGroup);

    return new VersionedFileStatus(
            transformedOwner,
            transformedGroup,
            permissions == null ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
                    : AbfsPermission.valueOf(permissions),
            hasAcl,
            contentLength,
            resourceIsDir,
            1,
            blockSize,
            parseLastModifiedTime(lastModified),
            path,
            eTag);
  }

  /**
   * @param path The list path.
<<<<<<< HEAD
   * @param tracingContext Tracks identifiers for request header
   * @return the entries in the path.
   * */
  @Override
  public FileStatus[] listStatus(final Path path, TracingContext tracingContext) throws IOException {
    return listStatus(path, null, tracingContext);
=======
   * @return the entries in the path.
   * */
  public FileStatus[] listStatus(final Path path) throws IOException {
    return listStatus(path, null);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  /**
   * @param path Path the list path.
   * @param startFrom the entry name that list results should start with.
   *                  For example, if folder "/folder" contains four files: "afile", "bfile", "hfile", "ifile".
   *                  Then listStatus(Path("/folder"), "hfile") will return "/folder/hfile" and "folder/ifile"
   *                  Notice that if startFrom is a non-existent entry name, then the list response contains
   *                  all entries after this non-existent entry in lexical order:
   *                  listStatus(Path("/folder"), "cfile") will return "/folder/hfile" and "/folder/ifile".
<<<<<<< HEAD
   * @param tracingContext Tracks identifiers for request header
   * @return the entries in the path start from  "startFrom" in lexical order.
   * */
  @InterfaceStability.Unstable
  @Override
  public FileStatus[] listStatus(final Path path, final String startFrom, TracingContext tracingContext) throws IOException {
    List<FileStatus> fileStatuses = new ArrayList<>();
    listStatus(path, startFrom, fileStatuses, true, null, tracingContext);
    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  @Override
  public String listStatus(final Path path, final String startFrom,
      List<FileStatus> fileStatuses, final boolean fetchAll,
      String continuation, TracingContext tracingContext) throws IOException {
    final Instant startAggregate = abfsPerfTracker.getLatencyInstant();
    long countAggregate = 0;
    boolean shouldContinue = true;

=======
   *
   * @return the entries in the path start from  "startFrom" in lexical order.
   * */
  @InterfaceStability.Unstable
  public FileStatus[] listStatus(final Path path, final String startFrom) throws IOException {
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
    LOG.debug("listStatus filesystem: {} path: {}, startFrom: {}",
            client.getFileSystem(),
            path,
            startFrom);

<<<<<<< HEAD
    final String relativePath = getRelativePath(path);

    if (continuation == null || continuation.isEmpty()) {
      // generate continuation token if a valid startFrom is provided.
      if (startFrom != null && !startFrom.isEmpty()) {
        continuation = getIsNamespaceEnabled(tracingContext)
            ? generateContinuationTokenForXns(startFrom)
            : generateContinuationTokenForNonXns(relativePath, startFrom);
=======
    final String relativePath = path.isRoot() ? AbfsHttpConstants.EMPTY_STRING : getRelativePath(path);
    String continuation = null;

    // generate continuation token if a valid startFrom is provided.
    if (startFrom != null && !startFrom.isEmpty()) {
      continuation = getIsNamespaceEnabled()
              ? generateContinuationTokenForXns(startFrom)
              : generateContinuationTokenForNonXns(path.isRoot() ? ROOT_PATH : relativePath, startFrom);
    }

    ArrayList<FileStatus> fileStatuses = new ArrayList<>();
    do {
      AbfsRestOperation op = client.listPath(relativePath, false, LIST_MAX_RESULTS, continuation);
      continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
      ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
      if (retrievedSchema == null) {
        throw new AbfsRestOperationException(
                AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                "listStatusAsync path not found",
                null, op.getResult());
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
      }
    }

<<<<<<< HEAD
    do {
      try (AbfsPerfInfo perfInfo = startTracking("listStatus", "listPath")) {
        AbfsRestOperation op = client.listPath(relativePath, false,
            abfsConfiguration.getListMaxResults(), continuation,
            tracingContext);
        perfInfo.registerResult(op.getResult());
        continuation = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_CONTINUATION);
        ListResultSchema retrievedSchema = op.getResult().getListResultSchema();
        if (retrievedSchema == null) {
          throw new AbfsRestOperationException(
                  AzureServiceErrorCode.PATH_NOT_FOUND.getStatusCode(),
                  AzureServiceErrorCode.PATH_NOT_FOUND.getErrorCode(),
                  "listStatusAsync path not found",
                  null, op.getResult());
        }

        long blockSize = abfsConfiguration.getAzureBlockSize();

        for (ListResultEntrySchema entry : retrievedSchema.paths()) {
          final String owner = identityTransformer.transformIdentityForGetRequest(entry.owner(), true, userName);
          final String group = identityTransformer.transformIdentityForGetRequest(entry.group(), false, primaryUserGroup);
          final FsPermission fsPermission = entry.permissions() == null
                  ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
                  : AbfsPermission.valueOf(entry.permissions());
          final boolean hasAcl = AbfsPermission.isExtendedAcl(entry.permissions());

          long lastModifiedMillis = 0;
          long contentLength = entry.contentLength() == null ? 0 : entry.contentLength();
          boolean isDirectory = entry.isDirectory() == null ? false : entry.isDirectory();
          if (entry.lastModified() != null && !entry.lastModified().isEmpty()) {
            lastModifiedMillis = DateTimeUtils.parseLastModifiedTime(
                entry.lastModified());
          }

          Path entryPath = new Path(File.separator + entry.name());
          entryPath = entryPath.makeQualified(this.uri, entryPath);

          fileStatuses.add(
                  new VersionedFileStatus(
                          owner,
                          group,
                          fsPermission,
                          hasAcl,
                          contentLength,
                          isDirectory,
                          1,
                          blockSize,
                          lastModifiedMillis,
                          entryPath,
                          entry.eTag()));
=======
      long blockSize = abfsConfiguration.getAzureBlockSize();

      for (ListResultEntrySchema entry : retrievedSchema.paths()) {
        final String owner = identityTransformer.transformIdentityForGetRequest(entry.owner(), true, userName);
        final String group = identityTransformer.transformIdentityForGetRequest(entry.group(), false, primaryUserGroup);
        final FsPermission fsPermission = entry.permissions() == null
                ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
                : AbfsPermission.valueOf(entry.permissions());
        final boolean hasAcl = AbfsPermission.isExtendedAcl(entry.permissions());

        long lastModifiedMillis = 0;
        long contentLength = entry.contentLength() == null ? 0 : entry.contentLength();
        boolean isDirectory = entry.isDirectory() == null ? false : entry.isDirectory();
        if (entry.lastModified() != null && !entry.lastModified().isEmpty()) {
          lastModifiedMillis = parseLastModifiedTime(entry.lastModified());
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
        }

        perfInfo.registerSuccess(true);
        countAggregate++;
        shouldContinue =
            fetchAll && continuation != null && !continuation.isEmpty();

        if (!shouldContinue) {
          perfInfo.registerAggregates(startAggregate, countAggregate);
        }
      }
    } while (shouldContinue);

    return continuation;
  }

  // generate continuation token for xns account
  private String generateContinuationTokenForXns(final String firstEntryName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(firstEntryName)
            && !firstEntryName.startsWith(AbfsHttpConstants.ROOT_PATH),
            "startFrom must be a dir/file name and it can not be a full path");

    StringBuilder sb = new StringBuilder();
    sb.append(firstEntryName).append("#$").append("0");

<<<<<<< HEAD
=======
    return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
  }

  // generate continuation token for xns account
  private String generateContinuationTokenForXns(final String firstEntryName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(firstEntryName)
            && !firstEntryName.startsWith(AbfsHttpConstants.ROOT_PATH),
            "startFrom must be a dir/file name and it can not be a full path");

    StringBuilder sb = new StringBuilder();
    sb.append(firstEntryName).append("#$").append("0");

>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
    CRC64 crc64 = new CRC64();
    StringBuilder token = new StringBuilder();
    token.append(crc64.compute(sb.toString().getBytes(StandardCharsets.UTF_8)))
            .append(SINGLE_WHITE_SPACE)
            .append("0")
            .append(SINGLE_WHITE_SPACE)
            .append(firstEntryName);

    return Base64.encode(token.toString().getBytes(StandardCharsets.UTF_8));
<<<<<<< HEAD
=======
  }

  // generate continuation token for non-xns account
  private String generateContinuationTokenForNonXns(final String path, final String firstEntryName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(firstEntryName)
            && !firstEntryName.startsWith(AbfsHttpConstants.ROOT_PATH),
            "startFrom must be a dir/file name and it can not be a full path");

    // Notice: non-xns continuation token requires full path (first "/" is not included) for startFrom
    final String startFrom = (path.isEmpty() || path.equals(ROOT_PATH))
            ? firstEntryName
            : path + ROOT_PATH + firstEntryName;

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(TOKEN_DATE_PATTERN, Locale.US);
    String date = simpleDateFormat.format(new Date());
    String token = String.format("%06d!%s!%06d!%s!%06d!%s!",
            path.length(), path, startFrom.length(), startFrom, date.length(), date);
    String base64EncodedToken = Base64.encode(token.getBytes(StandardCharsets.UTF_8));

    StringBuilder encodedTokenBuilder = new StringBuilder(base64EncodedToken.length() + 5);
    encodedTokenBuilder.append(String.format("%s!%d!", TOKEN_VERSION, base64EncodedToken.length()));

    for (int i = 0; i < base64EncodedToken.length(); i++) {
      char current = base64EncodedToken.charAt(i);
      if (CHAR_FORWARD_SLASH == current) {
        current = CHAR_UNDERSCORE;
      } else if (CHAR_PLUS == current) {
        current = CHAR_STAR;
      } else if (CHAR_EQUALS == current) {
        current = CHAR_HYPHEN;
      }
      encodedTokenBuilder.append(current);
    }

    return encodedTokenBuilder.toString();
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  // generate continuation token for non-xns account
  private String generateContinuationTokenForNonXns(String path, final String firstEntryName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(firstEntryName)
            && !firstEntryName.startsWith(AbfsHttpConstants.ROOT_PATH),
            "startFrom must be a dir/file name and it can not be a full path");

    // Notice: non-xns continuation token requires full path (first "/" is not included) for startFrom
    path = AbfsClient.getDirectoryQueryParameter(path);
    final String startFrom = (path.isEmpty() || path.equals(ROOT_PATH))
            ? firstEntryName
            : path + ROOT_PATH + firstEntryName;

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(TOKEN_DATE_PATTERN, Locale.US);
    String date = simpleDateFormat.format(new Date());
    String token = String.format("%06d!%s!%06d!%s!%06d!%s!",
            path.length(), path, startFrom.length(), startFrom, date.length(), date);
    String base64EncodedToken = Base64.encode(token.getBytes(StandardCharsets.UTF_8));

    StringBuilder encodedTokenBuilder = new StringBuilder(base64EncodedToken.length() + 5);
    encodedTokenBuilder.append(String.format("%s!%d!", TOKEN_VERSION, base64EncodedToken.length()));

    for (int i = 0; i < base64EncodedToken.length(); i++) {
      char current = base64EncodedToken.charAt(i);
      if (CHAR_FORWARD_SLASH == current) {
        current = CHAR_UNDERSCORE;
      } else if (CHAR_PLUS == current) {
        current = CHAR_STAR;
      } else if (CHAR_EQUALS == current) {
        current = CHAR_HYPHEN;
      }
      encodedTokenBuilder.append(current);
    }

    return encodedTokenBuilder.toString();
  }

  public void setOwner(final Path path, final String owner, final String group,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

<<<<<<< HEAD
    try (AbfsPerfInfo perfInfo = startTracking("setOwner", "setOwner")) {

      LOG.debug(
              "setOwner filesystem: {} path: {} owner: {} group: {}",
              client.getFileSystem(),
              path,
              owner,
              group);

      final String transformedOwner = identityTransformer.transformUserOrGroupForSetRequest(owner);
      final String transformedGroup = identityTransformer.transformUserOrGroupForSetRequest(group);

      final AbfsRestOperation op = client.setOwner(getRelativePath(path),
              transformedOwner,
              transformedGroup,
              tracingContext);

      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
=======
    LOG.debug(
            "setOwner filesystem: {} path: {} owner: {} group: {}",
            client.getFileSystem(),
            path.toString(),
            owner,
            group);

    final String transformedOwner = identityTransformer.transformUserOrGroupForSetRequest(owner);
    final String transformedGroup = identityTransformer.transformUserOrGroupForSetRequest(group);

    client.setOwner(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true), transformedOwner, transformedGroup);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  public void setPermission(final Path path, final FsPermission permission,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfo = startTracking("setPermission", "setPermission")) {

      LOG.debug(
              "setPermission filesystem: {} path: {} permission: {}",
              client.getFileSystem(),
              path,
              permission);

      final AbfsRestOperation op = client.setPermission(getRelativePath(path),
          String.format(AbfsHttpConstants.PERMISSION_FORMAT,
              permission.toOctal()), tracingContext);

      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
  }

  public void modifyAclEntries(final Path path, final List<AclEntry> aclSpec,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("modifyAclEntries", "getAclStatus")) {

<<<<<<< HEAD
      LOG.debug(
              "modifyAclEntries filesystem: {} path: {} aclSpec: {}",
              client.getFileSystem(),
              path,
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> modifyAclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      boolean useUpn = AbfsAclHelper.isUpnFormatAclEntries(modifyAclEntries);
=======
    identityTransformer.transformAclEntriesForSetRequest(aclSpec);
    final Map<String, String> modifyAclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
    boolean useUpn = AbfsAclHelper.isUpnFormatAclEntries(modifyAclEntries);

    final AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true), useUpn);
    final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

      String relativePath = getRelativePath(path);

<<<<<<< HEAD
      final AbfsRestOperation op = client
          .getAclStatus(relativePath, useUpn, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.modifyAclEntriesInternal(aclEntries, modifyAclEntries);
=======
    AbfsAclHelper.modifyAclEntriesInternal(aclEntries, modifyAclEntries);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("modifyAclEntries", "setAcl")) {
        final AbfsRestOperation setAclOp = client
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(aclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void removeAclEntries(final Path path, final List<AclEntry> aclSpec,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("removeAclEntries", "getAclStatus")) {

      LOG.debug(
              "removeAclEntries filesystem: {} path: {} aclSpec: {}",
              client.getFileSystem(),
              path,
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> removeAclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      boolean isUpnFormat = AbfsAclHelper.isUpnFormatAclEntries(removeAclEntries);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = client
          .getAclStatus(relativePath, isUpnFormat, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

<<<<<<< HEAD
      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));
=======
    identityTransformer.transformAclEntriesForSetRequest(aclSpec);
    final Map<String, String> removeAclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
    boolean isUpnFormat = AbfsAclHelper.isUpnFormatAclEntries(removeAclEntries);

    final AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true), isUpnFormat);
    final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

      AbfsAclHelper.removeAclEntriesInternal(aclEntries, removeAclEntries);

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("removeAclEntries", "setAcl")) {
        final AbfsRestOperation setAclOp = client
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(aclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void removeDefaultAcl(final Path path, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("removeDefaultAcl", "getAclStatus")) {

      LOG.debug(
              "removeDefaultAcl filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = client
          .getAclStatus(relativePath, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);
      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));
      final Map<String, String> defaultAclEntries = new HashMap<>();

      for (Map.Entry<String, String> aclEntry : aclEntries.entrySet()) {
        if (aclEntry.getKey().startsWith("default:")) {
          defaultAclEntries.put(aclEntry.getKey(), aclEntry.getValue());
        }
      }

<<<<<<< HEAD
      aclEntries.keySet().removeAll(defaultAclEntries.keySet());

      perfInfoGet.registerSuccess(true).finishTracking();
=======
    aclEntries.keySet().removeAll(defaultAclEntries.keySet());
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

      try (AbfsPerfInfo perfInfoSet = startTracking("removeDefaultAcl", "setAcl")) {
        final AbfsRestOperation setAclOp = client
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(aclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void removeAcl(final Path path, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

    try (AbfsPerfInfo perfInfoGet = startTracking("removeAcl", "getAclStatus")){

      LOG.debug(
              "removeAcl filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = client
          .getAclStatus(relativePath, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));
      final Map<String, String> newAclEntries = new HashMap<>();

      newAclEntries.put(AbfsHttpConstants.ACCESS_USER, aclEntries.get(AbfsHttpConstants.ACCESS_USER));
      newAclEntries.put(AbfsHttpConstants.ACCESS_GROUP, aclEntries.get(AbfsHttpConstants.ACCESS_GROUP));
      newAclEntries.put(AbfsHttpConstants.ACCESS_OTHER, aclEntries.get(AbfsHttpConstants.ACCESS_OTHER));

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("removeAcl", "setAcl")) {
        final AbfsRestOperation setAclOp = client
            .setAcl(relativePath, AbfsAclHelper.serializeAclSpec(newAclEntries),
                eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
  }

  public void setAcl(final Path path, final List<AclEntry> aclSpec,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

<<<<<<< HEAD
    try (AbfsPerfInfo perfInfoGet = startTracking("setAcl", "getAclStatus")) {

      LOG.debug(
              "setAcl filesystem: {} path: {} aclspec: {}",
              client.getFileSystem(),
              path,
              AclEntry.aclSpecToString(aclSpec));

      identityTransformer.transformAclEntriesForSetRequest(aclSpec);
      final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
      final boolean isUpnFormat = AbfsAclHelper.isUpnFormatAclEntries(aclEntries);

      String relativePath = getRelativePath(path);

      final AbfsRestOperation op = client
          .getAclStatus(relativePath, isUpnFormat, tracingContext);
      perfInfoGet.registerResult(op.getResult());
      final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

      final Map<String, String> getAclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

      AbfsAclHelper.setAclEntriesInternal(aclEntries, getAclEntries);

      perfInfoGet.registerSuccess(true).finishTracking();

      try (AbfsPerfInfo perfInfoSet = startTracking("setAcl", "setAcl")) {
        final AbfsRestOperation setAclOp =
                client.setAcl(relativePath,
                AbfsAclHelper.serializeAclSpec(aclEntries), eTag, tracingContext);
        perfInfoSet.registerResult(setAclOp.getResult())
                .registerSuccess(true)
                .registerAggregates(perfInfoGet.getTrackingStart(), GET_SET_AGGREGATE_COUNT);
      }
    }
=======
    LOG.debug(
            "setAcl filesystem: {} path: {} aclspec: {}",
            client.getFileSystem(),
            path.toString(),
            AclEntry.aclSpecToString(aclSpec));

    identityTransformer.transformAclEntriesForSetRequest(aclSpec);
    final Map<String, String> aclEntries = AbfsAclHelper.deserializeAclSpec(AclEntry.aclSpecToString(aclSpec));
    final boolean isUpnFormat = AbfsAclHelper.isUpnFormatAclEntries(aclEntries);

    final AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true), isUpnFormat);
    final String eTag = op.getResult().getResponseHeader(HttpHeaderConfigurations.ETAG);

    final Map<String, String> getAclEntries = AbfsAclHelper.deserializeAclSpec(op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL));

    AbfsAclHelper.setAclEntriesInternal(aclEntries, getAclEntries);

    client.setAcl(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true),
        AbfsAclHelper.serializeAclSpec(aclEntries), eTag);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  public AclStatus getAclStatus(final Path path, TracingContext tracingContext)
      throws IOException {
    if (!getIsNamespaceEnabled(tracingContext)) {
      throw new UnsupportedOperationException(
          "This operation is only valid for storage accounts with the hierarchical namespace enabled.");
    }

<<<<<<< HEAD
    try (AbfsPerfInfo perfInfo = startTracking("getAclStatus", "getAclStatus")) {

      LOG.debug(
              "getAclStatus filesystem: {} path: {}",
              client.getFileSystem(),
              path);

      AbfsRestOperation op = client
          .getAclStatus(getRelativePath(path), tracingContext);
      AbfsHttpOperation result = op.getResult();
      perfInfo.registerResult(result);

      final String transformedOwner = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_OWNER),
              true,
              userName);
      final String transformedGroup = identityTransformer.transformIdentityForGetRequest(
              result.getResponseHeader(HttpHeaderConfigurations.X_MS_GROUP),
              false,
              primaryUserGroup);

      final String permissions = result.getResponseHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS);
      final String aclSpecString = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL);

      final List<AclEntry> aclEntries = AclEntry.parseAclSpec(AbfsAclHelper.processAclString(aclSpecString), true);
      identityTransformer.transformAclEntriesForGetRequest(aclEntries, userName, primaryUserGroup);
      final FsPermission fsPermission = permissions == null ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
              : AbfsPermission.valueOf(permissions);

      final AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
      aclStatusBuilder.owner(transformedOwner);
      aclStatusBuilder.group(transformedGroup);

      aclStatusBuilder.setPermission(fsPermission);
      aclStatusBuilder.stickyBit(fsPermission.getStickyBit());
      aclStatusBuilder.addEntries(aclEntries);
      perfInfo.registerSuccess(true);
      return aclStatusBuilder.build();
    }
  }

  public void access(final Path path, final FsAction mode,
      TracingContext tracingContext) throws AzureBlobFileSystemException {
    LOG.debug("access for filesystem: {}, path: {}, mode: {}",
        this.client.getFileSystem(), path, mode);
    if (!this.abfsConfiguration.isCheckAccessEnabled()
        || !getIsNamespaceEnabled(tracingContext)) {
      LOG.debug("Returning; either check access is not enabled or the account"
          + " used is not namespace enabled");
      return;
    }
    try (AbfsPerfInfo perfInfo = startTracking("access", "checkAccess")) {
      final AbfsRestOperation op = this.client
          .checkAccess(getRelativePath(path), mode.SYMBOL, tracingContext);
      perfInfo.registerResult(op.getResult()).registerSuccess(true);
    }
=======
    LOG.debug(
            "getAclStatus filesystem: {} path: {}",
            client.getFileSystem(),
            path.toString());
    AbfsRestOperation op = client.getAclStatus(AbfsHttpConstants.FORWARD_SLASH + getRelativePath(path, true));
    AbfsHttpOperation result = op.getResult();

    final String transformedOwner = identityTransformer.transformIdentityForGetRequest(
            result.getResponseHeader(HttpHeaderConfigurations.X_MS_OWNER),
            true,
            userName);
    final String transformedGroup = identityTransformer.transformIdentityForGetRequest(
            result.getResponseHeader(HttpHeaderConfigurations.X_MS_GROUP),
            false,
            primaryUserGroup);

    final String permissions = result.getResponseHeader(HttpHeaderConfigurations.X_MS_PERMISSIONS);
    final String aclSpecString = op.getResult().getResponseHeader(HttpHeaderConfigurations.X_MS_ACL);

    final List<AclEntry> aclEntries = AclEntry.parseAclSpec(AbfsAclHelper.processAclString(aclSpecString), true);
    identityTransformer.transformAclEntriesForGetRequest(aclEntries, userName, primaryUserGroup);
    final FsPermission fsPermission = permissions == null ? new AbfsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL)
            : AbfsPermission.valueOf(permissions);

    final AclStatus.Builder aclStatusBuilder = new AclStatus.Builder();
    aclStatusBuilder.owner(transformedOwner);
    aclStatusBuilder.group(transformedGroup);

    aclStatusBuilder.setPermission(fsPermission);
    aclStatusBuilder.stickyBit(fsPermission.getStickyBit());
    aclStatusBuilder.addEntries(aclEntries);
    return aclStatusBuilder.build();
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  }

  public boolean isAtomicRenameKey(String key) {
    return isKeyForDirectorySet(key, azureAtomicRenameDirSet);
  }

  public boolean isInfiniteLeaseKey(String key) {
    if (azureInfiniteLeaseDirSet.isEmpty()) {
      return false;
    }
    return isKeyForDirectorySet(key, azureInfiniteLeaseDirSet);
  }

  /**
   * A on-off operation to initialize AbfsClient for AzureBlobFileSystem
   * Operations.
   *
   * @param uri            Uniform resource identifier for Abfs.
   * @param fileSystemName Name of the fileSystem being used.
   * @param accountName    Name of the account being used to access Azure
   *                       data store.
   * @param isSecure       Tells if https is being used or http.
   * @throws IOException
   */
  private void initializeClient(URI uri, String fileSystemName,
      String accountName, boolean isSecure)
      throws IOException {
    if (this.client != null) {
      return;
    }

    final URIBuilder uriBuilder = getURIBuilder(accountName, isSecure);

    final String url = uriBuilder.toString() + AbfsHttpConstants.FORWARD_SLASH + fileSystemName;

    URL baseUrl;
    try {
      baseUrl = new URL(url);
    } catch (MalformedURLException e) {
      throw new InvalidUriException(uri.toString());
    }

    SharedKeyCredentials creds = null;
    AccessTokenProvider tokenProvider = null;
    SASTokenProvider sasTokenProvider = null;

    if (authType == AuthType.OAuth) {
      AzureADAuthenticator.init(abfsConfiguration);
    }

    if (authType == AuthType.SharedKey) {
      LOG.trace("Fetching SharedKey credentials");
      int dotIndex = accountName.indexOf(AbfsHttpConstants.DOT);
      if (dotIndex <= 0) {
        throw new InvalidUriException(
                uri.toString() + " - account name is not fully qualified.");
      }
      creds = new SharedKeyCredentials(accountName.substring(0, dotIndex),
            abfsConfiguration.getStorageAccountKey());
    } else if (authType == AuthType.SAS) {
      LOG.trace("Fetching SAS token provider");
      sasTokenProvider = abfsConfiguration.getSASTokenProvider();
    } else {
      LOG.trace("Fetching token provider");
      tokenProvider = abfsConfiguration.getTokenProvider();
      ExtensionHelper.bind(tokenProvider, uri,
            abfsConfiguration.getRawConfiguration());
    }

    LOG.trace("Initializing AbfsClient for {}", baseUrl);
    if (tokenProvider != null) {
      this.client = new AbfsClient(baseUrl, creds, abfsConfiguration,
          tokenProvider,
          populateAbfsClientContext());
    } else {
      this.client = new AbfsClient(baseUrl, creds, abfsConfiguration,
          sasTokenProvider,
          populateAbfsClientContext());
    }
    LOG.trace("AbfsClient init complete");
  }

  /**
   * Populate a new AbfsClientContext instance with the desired properties.
   *
   * @return an instance of AbfsClientContext.
   */
  private AbfsClientContext populateAbfsClientContext() {
    return new AbfsClientContextBuilder()
        .withExponentialRetryPolicy(
            new ExponentialRetryPolicy(abfsConfiguration.getMaxIoRetries()))
        .withAbfsCounters(abfsCounters)
        .withAbfsPerfTracker(abfsPerfTracker)
        .build();
  }

  private String getOctalNotation(FsPermission fsPermission) {
    Preconditions.checkNotNull(fsPermission, "fsPermission");
    return String.format(AbfsHttpConstants.PERMISSION_FORMAT, fsPermission.toOctal());
  }

  private String getRelativePath(final Path path) {
    Preconditions.checkNotNull(path, "path");
    return path.toUri().getPath();
  }

  private long parseContentLength(final String contentLength) {
    if (contentLength == null) {
      return -1;
    }

    return Long.parseLong(contentLength);
  }

  private boolean parseIsDirectory(final String resourceType) {
    return resourceType != null
        && resourceType.equalsIgnoreCase(AbfsHttpConstants.DIRECTORY);
  }

<<<<<<< HEAD
=======
  private long parseLastModifiedTime(final String lastModifiedTime) {
    long parsedTime = 0;
    try {
      Date utcDate = new SimpleDateFormat(DATE_TIME_PATTERN, Locale.US).parse(lastModifiedTime);
      parsedTime = utcDate.getTime();
    } catch (ParseException e) {
      LOG.error("Failed to parse the date {}", lastModifiedTime);
    } finally {
      return parsedTime;
    }
  }

>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
  private String convertXmsPropertiesToCommaSeparatedString(final Hashtable<String, String> properties) throws
          CharacterCodingException {
    StringBuilder commaSeparatedProperties = new StringBuilder();

    final CharsetEncoder encoder = Charset.forName(XMS_PROPERTIES_ENCODING).newEncoder();

    for (Map.Entry<String, String> propertyEntry : properties.entrySet()) {
      String key = propertyEntry.getKey();
      String value = propertyEntry.getValue();

      Boolean canEncodeValue = encoder.canEncode(value);
      if (!canEncodeValue) {
        throw new CharacterCodingException();
      }

      String encodedPropertyValue = Base64.encode(encoder.encode(CharBuffer.wrap(value)).array());
      commaSeparatedProperties.append(key)
              .append(AbfsHttpConstants.EQUAL)
              .append(encodedPropertyValue);

      commaSeparatedProperties.append(AbfsHttpConstants.COMMA);
    }

    if (commaSeparatedProperties.length() != 0) {
      commaSeparatedProperties.deleteCharAt(commaSeparatedProperties.length() - 1);
    }

    return commaSeparatedProperties.toString();
  }

  private Hashtable<String, String> parseCommaSeparatedXmsProperties(String xMsProperties) throws
          InvalidFileSystemPropertyException, InvalidAbfsRestOperationException {
    Hashtable<String, String> properties = new Hashtable<>();

    final CharsetDecoder decoder = Charset.forName(XMS_PROPERTIES_ENCODING).newDecoder();

    if (xMsProperties != null && !xMsProperties.isEmpty()) {
      String[] userProperties = xMsProperties.split(AbfsHttpConstants.COMMA);

      if (userProperties.length == 0) {
        return properties;
      }

      for (String property : userProperties) {
        if (property.isEmpty()) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        String[] nameValue = property.split(AbfsHttpConstants.EQUAL, 2);
        if (nameValue.length != 2) {
          throw new InvalidFileSystemPropertyException(xMsProperties);
        }

        byte[] decodedValue = Base64.decode(nameValue[1]);

        final String value;
        try {
          value = decoder.decode(ByteBuffer.wrap(decodedValue)).toString();
        } catch (CharacterCodingException ex) {
          throw new InvalidAbfsRestOperationException(ex);
        }
        properties.put(nameValue[0], value);
      }
    }

    return properties;
  }

  private boolean isKeyForDirectorySet(String key, Set<String> dirSet) {
    for (String dir : dirSet) {
      if (dir.isEmpty() || key.startsWith(dir + AbfsHttpConstants.FORWARD_SLASH)) {
        return true;
      }

      try {
        URI uri = new URI(dir);
        if (null == uri.getAuthority()) {
          if (key.startsWith(dir + "/")){
            return true;
          }
        }
      } catch (URISyntaxException e) {
        LOG.info("URI syntax error creating URI for {}", dir);
      }
    }

    return false;
  }

  private AbfsPerfInfo startTracking(String callerName, String calleeName) {
    return new AbfsPerfInfo(abfsPerfTracker, callerName, calleeName);
  }

  private static class VersionedFileStatus extends FileStatus {
    private final String version;

    VersionedFileStatus(
            final String owner, final String group, final FsPermission fsPermission, final boolean hasAcl,
            final long length, final boolean isdir, final int blockReplication,
            final long blocksize, final long modificationTime, final Path path,
            String version) {
      super(length, isdir, blockReplication, blocksize, modificationTime, 0,
              fsPermission,
              owner,
              group,
              null,
              path,
              hasAcl, false, false);

      this.version = version;
    }

    /** Compare if this object is equal to another object.
     * @param   obj the object to be compared.
     * @return  true if two file status has the same path name; false if not.
     */
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof FileStatus)) {
        return false;
      }

      FileStatus other = (FileStatus) obj;

      if (!this.getPath().equals(other.getPath())) {// compare the path
        return false;
      }

      if (other instanceof VersionedFileStatus) {
        return this.version.equals(((VersionedFileStatus) other).version);
      }

      return true;
    }

    /**
     * Returns a hash code value for the object, which is defined as
     * the hash code of the path name.
     *
     * @return  a hash code value for the path name and version
     */
    @Override
    public int hashCode() {
      int hash = getPath().hashCode();
      hash = 89 * hash + (this.version != null ? this.version.hashCode() : 0);
      return hash;
    }

    /**
     * Returns the version of this FileStatus
     *
     * @return  a string value for the FileStatus version
     */
    public String getVersion() {
      return this.version;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "VersionedFileStatus{");
      sb.append(super.toString());
      sb.append("; version='").append(version).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  @VisibleForTesting
  AbfsClient getClient() {
    return this.client;
  }

  @VisibleForTesting
  void setClient(AbfsClient client) {
    this.client = client;
  }

  @VisibleForTesting
  void setNamespaceEnabled(Trilean isNamespaceEnabled){
    this.isNamespaceEnabled = isNamespaceEnabled;
  }

  private void updateInfiniteLeaseDirs() {
    this.azureInfiniteLeaseDirSet = new HashSet<>(Arrays.asList(
        abfsConfiguration.getAzureInfiniteLeaseDirs().split(AbfsHttpConstants.COMMA)));
    // remove the empty string, since isKeyForDirectory returns true for empty strings
    // and we don't want to default to enabling infinite lease dirs
    this.azureInfiniteLeaseDirSet.remove("");
  }

  private AbfsLease maybeCreateLease(String relativePath, TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    boolean enableInfiniteLease = isInfiniteLeaseKey(relativePath);
    if (!enableInfiniteLease) {
      return null;
    }
    AbfsLease lease = new AbfsLease(client, relativePath, tracingContext);
    leaseRefs.put(lease, null);
    return lease;
  }

  @VisibleForTesting
  boolean areLeasesFreed() {
    for (AbfsLease lease : leaseRefs.keySet()) {
      if (lease != null && !lease.isFreed()) {
        return false;
      }
    }
    return true;
  }
}
