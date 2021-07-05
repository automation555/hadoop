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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

/**
 * FileSystem operations which are needed to generate the task manifest.
 */
public interface StoreOperations extends Closeable {

  /**
   * Forward to {@link FileSystem#getFileStatus(Path)}.
   * @param path path
   * @return status
   * @throws IOException failure.
   */
  FileStatus getFileStatus(Path path) throws IOException;

  /**
   * Forward to {@link FileSystem#delete(Path, boolean)}.
   * If it returns without an error: there is nothing at
   * the end of the path.
   * @param path path
   * @param recursive recursive delete.
   * @return true if the path was deleted.
   * @throws IOException failure.
   */
  boolean delete(Path path, boolean recursive)
      throws IOException;

  /**
   * Forward to {@link FileSystem#mkdirs(Path)}.
   * Usual "what does 'false' mean" ambiguity.
   * @param path path
   * @return true if the directory was created.
   * @throws IOException failure.
   */
  boolean mkdirs(Path path)
      throws IOException;

  /**
   * Forward to {@link FileSystem#rename(Path, Path)}.
   * Usual "what does 'false' mean" ambiguity.
   * @param source source file
   * @param dest destination path -which must not exist.
   * @return true if the directory was created.
   * @throws IOException failure.
   */
  boolean renameFile(Path source, Path dest)
      throws IOException;

  /**
   * List the directory.
   * @param path path to list.
   * @return an iterator over the results.
   * @throws IOException any immediate failure.
   */
  RemoteIterator<FileStatus> listStatusIterator(Path path)
      throws IOException;

  /**
   * Load a task manifest from the store.
   * with a real FS, this is done with
   * {@link TaskManifest#load(FileSystem, FileStatus)}
   *
   * @param st status with the path and other data.
   * @return the manifest
   * @throws IOException failure to load/parse
   */
  TaskManifest loadTaskManifest(FileStatus st) throws IOException;

  /**
   * Save a task manifest by create(); there's no attempt at
   * renaming anything here.
   * @param manifestData the manifest/success file
   * @param path temp path for the initial save
   * @param overwrite should create(overwrite=true) be used?
   * @throws IOException failure to load/parse
   */
  void save(AbstractManifestData manifestData,
      Path path, boolean overwrite) throws IOException;

  /**
   * Move a directory to trash, with the jobID as its name.
   * IOExceptions are caught and logged; the method then
   * returns false.
   * @param jobId job ID.
   * @param path path to move, assumed to be _temporary
   * @return true if the rename worked.
   */
  boolean moveToTrash(String jobId, Path path);
}
