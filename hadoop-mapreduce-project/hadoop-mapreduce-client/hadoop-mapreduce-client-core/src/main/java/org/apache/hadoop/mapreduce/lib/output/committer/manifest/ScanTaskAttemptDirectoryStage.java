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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.snapshotIOStatistics;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DIRECTORY_SCAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SCAN_DIRECTORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.createTaskManifest;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.maybeAddIOStatistics;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry.dirEntry;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry.fileEntry;

/**
 * Stage to scan a directory tree and build a task manifest.
 * This is executed by the task committer.
 */
public final class ScanTaskAttemptDirectoryStage
    extends AbstractJobCommitStage<String, TaskManifest> {

  private static final Logger LOG = LoggerFactory.getLogger(
      ScanTaskAttemptDirectoryStage.class);

  public ScanTaskAttemptDirectoryStage(
      final StageConfig stageConfig) {
    super(true, stageConfig, OP_STAGE_TASK_SCAN_DIRECTORY, false);
  }

  /**
   * Build the Manifest.
   * @return the manifest
   * @throws IOException failure.
   */
  @Override
  protected TaskManifest executeStage(final String arguments)
      throws IOException {

    final String taskAttemptId = getRequiredTaskAttemptId();
    final Path taskAttemptDir = getRequiredTaskAttemptDir();
    final TaskManifest manifest = createTaskManifest(getStageConfig());

    LOG.info("Task Attempt {} scanning directory {}",
        taskAttemptId, taskAttemptDir);
    // build up the manifest statistics, picking up whatever statistic
    // names the store uses in its listing (if any)
    final IOStatisticsSnapshot manifestStats = snapshotIOStatistics();
    final int d = scanDirectoryTree(manifest, taskAttemptDir,
        getDestinationDir(),
        manifestStats,
        0);
    List<FileOrDirEntry> filesToCommit = manifest.getFilesToCommit();
    long size = 0;
    for (FileOrDirEntry entry : filesToCommit) {
      size += entry.getSize();
    }
    LOG.info("Task Attempt {} directory {};",
        taskAttemptId,
        taskAttemptDir);
    LOG.info("Contained {} file(s); data size {}",
        filesToCommit.size(),
        size);
    LOG.info("Directory count = {}; maximum depth {}",
        manifest.getDirectoriesToCreate().size(),
        d);

    // save a snapshot of the IO Statistics
    manifestStats.aggregate(getIOStatistics());
    manifest.setIOStatistics(manifestStats);

    return manifest;
  }

  /**
   * Recursively scan a directory tree.
   * The manifest will contain all files to rename
   * (source and dest) and directories to create.
   * All files are processed before any of the subdirs are.
   * This helps in statistics gathering.
   * There's some optimizations which could be done with async
   * fetching of the iterators of those subdirs, but as this
   * is generally off-critical path then that "enhancement"
   * can be postponed until data suggests this needs improvement.
   * @param manifest manifest to update
   * @param srcDir dir to scan
   * @param destDir destination directory
   * @param depth depth from the task attempt dir.
   * @return the maximum depth of child directories
   * @throws IOException IO failure.
   */
  private int scanDirectoryTree(
      TaskManifest manifest,
      Path srcDir,
      Path destDir,
      IOStatisticsAggregator ios,
      int depth) throws IOException {

    // generate some task progress in case directory scanning is very slow.
    progress();

    int maxDepth = 0;
    int files = 0;
    List<FileStatus> subdirs = new ArrayList<>();
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "Task Attempt %s source dir %s, dest dir %s",
        getTaskAttemptId(), srcDir, destDir)) {

      // list the directory. This may block until the listing is complete,
      // or, if the FS does incremental or asynchronous fetching, until the
      // first page of results is ready.
      final RemoteIterator<FileStatus> listing =
          trackDuration(getIOStatistics(), OP_DIRECTORY_SCAN, () ->
              listStatusIterator(srcDir));

      while (listing.hasNext()) {
        final FileStatus st = listing.next();
        if (st.isFile()) {
          files++;
          final FileOrDirEntry entry = fileEntry(st, destDir);
          manifest.addFileToCommit(entry);
          LOG.debug("To rename: {}", entry);
        } else {
          if (st.isDirectory()) {
            // will need to scan this directory too.
            subdirs.add(st);
          } else {
            // some other object. ignoring
            LOG.info("Ignoring FS object {}", st);
          }
        }
      }
      // add any statistics provided by the listing.
      maybeAddIOStatistics(ios, listing);
    }
    // if files were added and this is not the base directory.
    // it will need to be created.
    // TODO: reinstate once debugged
    if (depth > 0 /*&& files > 0*/) {
      manifest.addDirectory(dirEntry(srcDir, destDir));
    }

    // now scan the subdirectories
    LOG.debug("Number of subdirectories under {} found: {}",
        srcDir, subdirs.size());
    for (FileStatus st: subdirs) {
      Path destSubDir = new Path(destDir, st.getPath().getName());
      final int d = scanDirectoryTree(manifest, st.getPath(), destSubDir,
          ios, depth + 1);
      maxDepth = Math.max(maxDepth, d);
    }

    return 1 + maxDepth;
  }


}
