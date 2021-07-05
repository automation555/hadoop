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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_CREATE_DIRECTORIES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CREATE_TARGET_DIRS;

/**
 * Prepare the destination directory tree, by making as few IO calls as
 * possible.
 * Each task manifest's directories are combined with those of the other tasks
 * to build a set of all directories which are needed, without duplicates.
 * Parent directories are then removed, as they will be implicitly created.
 * This leaves a set of directories which must exist by the end of each stage.
 * The operations of probing for and creating these are fed to the IO
 * Processors, as they can now be done independently.
 *
 * The stage returns the list of directories created.
 */
public class PrepareDirectoriesStage extends
    AbstractJobCommitStage<List<TaskManifest>, List<Path>> {

  private static final Logger LOG = LoggerFactory.getLogger(
      PrepareDirectoriesStage.class);

  /**
   * Directories as a map of (path, created).
   */
  private final Map<Path, Path> dirMap = new ConcurrentHashMap<>();

  public PrepareDirectoriesStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_CREATE_TARGET_DIRS, true);
    dirMap.put(getDestinationDir(), getDestinationDir());
  }

  @Override
  protected List<Path> executeStage(
      final List<TaskManifest> taskManifests)
      throws IOException {

    LOG.info("Creating directories");
    final List<Path> directories = createAllDirectories(taskManifests);
    LOG.debug("Created {} directories", directories.size());
    return directories;
  }

  /**
   * For each task, build the list of directories it wants.
   * @param taskManifests task manifests
   * @return the list of paths which have been created.
   */
  private List<Path> createAllDirectories(List<TaskManifest> taskManifests)
      throws IOException {

    Set<Path> directoriesToCreate = new HashSet<>();
    for (TaskManifest task : taskManifests) {
      final List<FileOrDirEntry> dirEntries
          = task.getDirectoriesToCreate();
      for (FileOrDirEntry entry : dirEntries) {
        // add the dest path
        directoriesToCreate.add(entry.getDestPath());
      }
    }


    // the set is all directories which need to exist across all
    // tasks.

    // Go through dir map and insert an entry for all
    // rename wants to rename a file to a/b/c/d.orc
    // is list of dirs to create is [a/b, a/b/c]
    // but a/b == exists at the dest and is a file
    /*
    To maintain the exact behavior of v1
     */


    // there are no duplicates: creating the set is sufficient
    // to filter all these out.
    final List<Path> createdPaths = new ArrayList<>(directoriesToCreate.size());

    // TODO: somehow create all parent dirs (or at least delete files
    // at those locations) be for

    // now probe for and create. There are some marginal optimizations such
    // as removing any parent entries from the dest tree.
    //
    trackDurationOfInvocation(getIOStatistics(), OP_CREATE_DIRECTORIES, () -> {
      TaskPool.foreach(directoriesToCreate)
          .executeWith(getIOProcessors())
          .stopOnFailure()
          .run(dir -> {
            if (maybeCreateOneDirectory(dir)) {
              synchronized (createdPaths) {
                createdPaths.add(dir);
              }
            }
          });
    });
    return createdPaths;
  }

  /**
   * Set up the destination directories while trying to minimize the amount
   * of duplicate IO.
   * @param path path to create
   * @throws IOException IO Failure.
   */
  private boolean maybeCreateOneDirectory(Path path) throws IOException {
    // if a directory is in the map: return.
    boolean create;
    if (dirMap.get(path) == null) {
      // there's no entry in the map.

      // TODO: only do GFS if mkdirs() fails.

      // See if it exists
      final FileStatus st = getFileStatusOrNull(path);
      if (st != null) {
        if (st.isDirectory()) {
          // is good.
          dirMap.put(path, path);
          create = false;
        } else {
          // is bad: delete a file
          LOG.info("Deleting file where a directory should go: {}", st);
          delete(path, false);
          create = true;
        }
      } else {
        // nothing found
        create = true;
      }
    } else {
      // it is in the map because either it has been created in another thread
      // or because a child of it has been created in another thread and the
      // map was updated.
      create = false;
    }
    if (create) {
      // add the dir to the map, along with all parents.
      addDirectoryAndParentsToDirectoryMap(path);
      // create the directory
      if (!mkdirs(path, false)) {
        // two possible outcomes. Something went very wrong
        // or the directory was created by an action in a parallel thread

        // mkdirs() could also fail if there is a parent path which
        // is a file
        directoryMustExist("Creating directory ", path);
      }
    }
    return create;

  }

  private void addDirectoryAndParentsToDirectoryMap(Path path) {
    dirMap.put(path, path);
    addParentsToDirectoryMap(path);
  }

  private void addParentsToDirectoryMap(final Path path) {
    Path parent = path.getParent();
    while (!parent.isRoot() && !getDestinationDir().equals(parent)) {
      dirMap.put(parent, parent);
      parent = parent.getParent();
    }
  }

}
