/*
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

package org.apache.hadoop.fs;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
<<<<<<< HEAD
import java.util.concurrent.CompletableFuture;
=======

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * MultipartUploader is an interface for copying files multipart and across
<<<<<<< HEAD
 * multiple nodes.
 * <p></p>
 * The interface extends {@link IOStatisticsSource} so that there is no
 * need to cast an instance to see if is a source of statistics.
 * However, implementations MAY return null for their actual statistics.
=======
 * multiple nodes. Users should:
 * <ol>
 *   <li>Initialize an upload.</li>
 *   <li>Upload parts in any order.</li>
 *   <li>Complete the upload in order to have it materialize in the destination
 *   FS.</li>
 * </ol>
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
<<<<<<< HEAD
public interface MultipartUploader extends Closeable,
    IOStatisticsSource {

=======
public abstract class MultipartUploader implements Closeable {
  public static final Logger LOG =
      LoggerFactory.getLogger(MultipartUploader.class);
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776

  /**
   * Perform any cleanup.
   * The upload is not required to support any operations after this.
   * @throws IOException problems on close.
   */
  @Override
  public void close() throws IOException {
  }

  /**
   * Initialize a multipart upload.
   * @param filePath Target path for upload.
   * @return unique identifier associating part uploads.
   * @throws IOException IO failure
   */
  CompletableFuture<UploadHandle> startUpload(Path filePath)
      throws IOException;

  /**
   * Put part as part of a multipart upload.
   * It is possible to have parts uploaded in any order (or in parallel).
<<<<<<< HEAD
   * @param uploadId Identifier from {@link #startUpload(Path)}.
   * @param partNumber Index of the part relative to others.
   * @param filePath Target path for upload (as {@link #startUpload(Path)}).
=======
   * @param filePath Target path for upload (same as {@link #initialize(Path)}).
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
   * @param inputStream Data for this part. Implementations MUST close this
   * stream after reading in the data.
   * @param lengthInBytes Target length to read from the stream.
   * @return unique PartHandle identifier for the uploaded part.
   * @throws IOException IO failure
   */
  CompletableFuture<PartHandle> putPart(
      UploadHandle uploadId,
      int partNumber,
      Path filePath,
      InputStream inputStream,
      long lengthInBytes)
      throws IOException;

  /**
   * Complete a multipart upload.
<<<<<<< HEAD
   * @param uploadId Identifier from {@link #startUpload(Path)}.
   * @param filePath Target path for upload (as {@link #startUpload(Path)}.
   * @param handles non-empty map of part number to part handle.
   *          from {@link #putPart(UploadHandle, int, Path, InputStream, long)}.
   * @return unique PathHandle identifier for the uploaded file.
   * @throws IOException IO failure
   */
  CompletableFuture<PathHandle> complete(
      UploadHandle uploadId,
      Path filePath,
      Map<Integer, PartHandle> handles)
=======
   * @param filePath Target path for upload (same as {@link #initialize(Path)}.
   * @param handles non-empty map of part number to part handle.
   *          from {@link #putPart(Path, InputStream, int, UploadHandle, long)}.
   * @param multipartUploadId Identifier from {@link #initialize(Path)}.
   * @return unique PathHandle identifier for the uploaded file.
   * @throws IOException IO failure
   */
  public abstract PathHandle complete(Path filePath,
      Map<Integer, PartHandle> handles,
      UploadHandle multipartUploadId)
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
      throws IOException;

  /**
   * Aborts a multipart upload.
   * @param uploadId Identifier from {@link #startUpload(Path)}.
   * @param filePath Target path for upload (same as {@link #startUpload(Path)}.
   * @throws IOException IO failure
   * @return a future; the operation will have completed
   */
  CompletableFuture<Void> abort(UploadHandle uploadId, Path filePath)
      throws IOException;

  /**
<<<<<<< HEAD
   * Best effort attempt to aborts multipart uploads under a path.
   * Not all implementations support this, and those which do may
   * be vulnerable to eventually consistent listings of current uploads
   * -some may be missed.
   * @param path path to abort uploads under.
   * @return a future to the number of entries aborted;
   * -1 if aborting is unsupported
   * @throws IOException IO failure
   */
  CompletableFuture<Integer> abortUploadsUnderPath(Path path) throws IOException;

=======
   * Utility method to validate uploadIDs.
   * @param uploadId Upload ID
   * @throws IllegalArgumentException invalid ID
   */
  protected void checkUploadId(byte[] uploadId)
      throws IllegalArgumentException {
    checkArgument(uploadId != null, "null uploadId");
    checkArgument(uploadId.length > 0,
        "Empty UploadId is not valid");
  }

  /**
   * Utility method to validate partHandles.
   * @param partHandles handles
   * @throws IllegalArgumentException if the parts are invalid
   */
  protected void checkPartHandles(Map<Integer, PartHandle> partHandles) {
    checkArgument(!partHandles.isEmpty(),
        "Empty upload");
    partHandles.keySet()
        .stream()
        .forEach(key ->
            checkArgument(key > 0,
                "Invalid part handle index %s", key));
  }

  /**
   * Check all the arguments to the
   * {@link #putPart(Path, InputStream, int, UploadHandle, long)} operation.
   * @param filePath Target path for upload (same as {@link #initialize(Path)}).
   * @param inputStream Data for this part. Implementations MUST close this
   * stream after reading in the data.
   * @param partNumber Index of the part relative to others.
   * @param uploadId Identifier from {@link #initialize(Path)}.
   * @param lengthInBytes Target length to read from the stream.
   * @throws IllegalArgumentException invalid argument
   */
  protected void checkPutArguments(Path filePath,
      InputStream inputStream,
      int partNumber,
      UploadHandle uploadId,
      long lengthInBytes) throws IllegalArgumentException {
    checkArgument(filePath != null, "null filePath");
    checkArgument(inputStream != null, "null inputStream");
    checkArgument(partNumber > 0, "Invalid part number: %d", partNumber);
    checkArgument(uploadId != null, "null uploadId");
    checkArgument(lengthInBytes >= 0, "Invalid part length: %d", lengthInBytes);
  }
>>>>>>> a6df05bf5e24d04852a35b096c44e79f843f4776
}
