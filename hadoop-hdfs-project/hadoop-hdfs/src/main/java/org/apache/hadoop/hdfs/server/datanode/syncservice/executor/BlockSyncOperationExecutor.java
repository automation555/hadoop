/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.syncservice.executor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BBUploadHandle;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.MultipartUploaderFactory;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockInputStream;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

/**
 * BlockSyncOperationExecutor writes the blocks to the sync service remote
 * endpoint.
 */
public class BlockSyncOperationExecutor  {

  public static final Logger LOG =
      LoggerFactory.getLogger(BlockSyncOperationExecutor.class);

  private Configuration conf;
  private BiFunction<LocatedBlock, Configuration, BlockReader>
      createBlockReader;
  private Function<FileSystem, MultipartUploader> multipartUploaderSupplier;

  @VisibleForTesting
  BlockSyncOperationExecutor(Configuration conf,
      BiFunction<LocatedBlock, Configuration, BlockReader> createBlockReader,
      Function<FileSystem, MultipartUploader> multipartUploaderSupplier) {
    this.conf = conf;
    this.createBlockReader = createBlockReader;
    this.multipartUploaderSupplier = multipartUploaderSupplier;
  }

  public static BlockSyncOperationExecutor createOnDataNode(Configuration conf,
      BiFunction<LocatedBlock, Configuration, BlockReader> createBlockReader) {
    return new BlockSyncOperationExecutor(conf,
        createBlockReader,
        fs -> {
          try {
            return MultipartUploaderFactory.get(fs, conf);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  public SyncTaskExecutionResult execute(BlockSyncTask blockSyncTask)
      throws Exception {
    LOG.info("Executing MetadataSyncTask {} (on {})",
        blockSyncTask.getSyncTaskId(), blockSyncTask.getRemoteURI());

      return doMultiPartPart(
          blockSyncTask.getRemoteURI(),
          blockSyncTask.getLocatedBlocks(),
          blockSyncTask.getPartNumber(),
          blockSyncTask.getUploadHandle(),
          blockSyncTask.getOffset(),
          blockSyncTask.getLength());
  }

  private SyncTaskExecutionResult doMultiPartPart(URI uri,
      List<LocatedBlock> locatedBlocks, int partNumber, byte[] uploadHandle,
      int offset, long length) throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    Path filePath = new Path(uri);
    Vector<InputStream> inputStreams = new Vector<>(locatedBlocks.size());
    for (int i = 0; i < locatedBlocks.size(); ++i) {
      LocatedBlock locatedBlock = locatedBlocks.get(i);
      BlockReader reader = createBlockReader.apply(locatedBlock, conf);
      if (i == 0) {
        reader.skip(offset);
      }
      BlockInputStream inputStream = new BlockInputStream(reader);
      inputStreams.add(inputStream);
    }
    Enumeration<InputStream> streamEnumeration = inputStreams.elements();
    SequenceInputStream inputStream =
        new SequenceInputStream(streamEnumeration);
    MultipartUploader mpu = multipartUploaderSupplier.apply(fs);
    PartHandle partHandle = mpu.putPart(filePath, inputStream,
        partNumber, BBUploadHandle.from(ByteBuffer.wrap(uploadHandle)), length);
    return new SyncTaskExecutionResult(partHandle.bytes(), length);
  }
}

