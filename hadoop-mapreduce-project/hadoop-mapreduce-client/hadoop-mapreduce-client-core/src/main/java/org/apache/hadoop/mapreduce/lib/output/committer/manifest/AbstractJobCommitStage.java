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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.functional.RemoteIterators;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_GET_FILE_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_LIST_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_RENAME;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_LOAD_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_RENAME_FILE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_SAVE_TASK_MANIFEST;

/**
 * A Stage in Task/Job Commit.
 * A stage can be executed once only, creating the return value of the
 * {@link #apply(Object)} method, and, potentially, updating the state of the
 * store via {@link StoreOperations}.
 * IOStatistics will also be updated.
 * Stages are expected to be combined to form the commit protocol.
 * @param <IN> Type of arguments to the stage.
 * @param <OUT> Type of result.
 */
public abstract class AbstractJobCommitStage<IN, OUT>
    implements JobStage<IN, OUT> {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractJobCommitStage.class);

  /**
   * Is this a task stage? If so, toString() includes task
   * info..
   */
  private final boolean isTaskStage;

  /**
   * Configuration of all the stages in the ongoing committer
   * operation.
   */
  private final StageConfig stageConfig;

  /**
   * Name of the stage for statistics and logging.
   */
  private final String stageStatisticName;

  /**
   * Callbacks to update store.
   * This is not made visible to the stages; they must
   * go through the wrapper classes in this class, which
   * add statistics and logging.
   */
  private final StoreOperations operations;

  /**
   * Submitter for doing IO against the store.
   */
  private final TaskPool.Submitter ioProcessors;

  /**
   * Used to stop any re-entrancy of the rename.
   * This is an execute-once operation.
   */
  private final AtomicBoolean executed = new AtomicBoolean(false);

  /**
   * Constructor.
   * @param isTaskStage Is this a task stage?
   * @param stageConfig stage-independent configuration.
   * @param stageStatisticName name of the stage for statistics/logging
   * @param requireIOProcessors are the IO processors required?
   */
  protected AbstractJobCommitStage(
      final boolean isTaskStage,
      final StageConfig stageConfig,
      final String stageStatisticName,
      final boolean requireIOProcessors) {
    this.isTaskStage = isTaskStage;
    this.stageStatisticName = stageStatisticName;
    this.stageConfig = stageConfig;
    requireNonNull(stageConfig.getDestinationDir(), "Destination Directory");
    requireNonNull(stageConfig.getJobId(), "Job ID");
    requireNonNull(stageConfig.getJobAttemptDir(), "Job attempt directory");
    this.operations = requireNonNull(stageConfig.getOperations(),
        "Operations callbacks");
    // and the processors of work if required.
    this.ioProcessors = bindProcessor(
        requireIOProcessors,
        stageConfig.getIoProcessors());
    if (isTaskStage) {
      // force fast failure.
      getRequiredTaskId();
      getRequiredTaskAttemptId();
      getRequiredTaskAttemptDir();
    }
  }

  /**
   * Bind to the processor if it is required.
   * @param required is the processor required?
   * @param processor processor
   * @return the processor binding
   * @throws NullPointerException if required == true and processor is null.
   */
  private TaskPool.Submitter bindProcessor(boolean required,
      TaskPool.Submitter processor) {
    return required
        ? requireNonNull(processor, "required IO processor is null")
        : null;
  }

  /**
   * Stage entry point.
   * Verifies that this is the first and only time the stage is invoked,
   * then calls {@link #executeStage(Object)} for the subclass
   * to perform its part of the commit protocol.
   * The duration of the stage is collected as a statistic, and its
   * entry/exit logged at INFO.
   * @param arguments arguments to the function.
   * @return the result.
   * @throws IOException failures.
   */
  @Override
  public final OUT apply(final IN arguments) throws IOException {
    executeOnlyOnce();
    progress();
    try (DurationInfo ignored = new DurationInfo(LOG,
        false, "Executing stage %s", stageStatisticName)) {
      return trackDuration(getIOStatistics(), stageStatisticName, () ->
          executeStage(arguments));
    } finally {
      progress();
    }
  }

  /**
   * The work of a stage.
   * Executed exactly once.
   * @param arguments arguments to the function.
   * @return the result.
   * @throws IOException failures.
   */
  protected abstract OUT executeStage(IN arguments) throws IOException;

  /**
   * Check that the operation has not been invoked twice.
   * This is an atomic check.
   * @throws IllegalStateException on a second invocation.
   */
  private void executeOnlyOnce() {
    Preconditions.checkState(
        !executed.getAndSet(true),
        "Stage attempted twice");
  }

  /**
   * The stage statistic name.
   * @return stage name.
   */
  public String getStageStatisticName() {
    return stageStatisticName;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbstractJobCommitStage{");
    sb.append(isTaskStage ? "Task Stage" : "Job Stage");
    sb.append(" name='").append(stageStatisticName).append('\'');
    if (isTaskStage) {
      sb.append(", Task Attempt ID=")
          .append(getTaskAttemptId());
    } else {
      sb.append(", ID =")
          .append(getJobId())
          .append("; attempt #")
          .append(getJobAttemptNumber());
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * The stage configuration.
   * @return the stage configuration used by this stage.
   */
  protected StageConfig getStageConfig() {
    return stageConfig;
  }

  /**
   * The IOStatistics are shared across all uses of the
   * StageConfig.
   * @return the (possibly shared) IOStatistics.
   */
  @Override
  public final IOStatisticsStore getIOStatistics() {
    return stageConfig.getIOStatistics();
  }

  /**
   * Call progress() on any Progressable passed in.
   */
  protected final void progress() {
    if (stageConfig.getProgressable() != null) {
      stageConfig.getProgressable().progress();
    }
  }

  /**
   * Get a file status value or, if the path doesn't exist, return null.
   * @param path path
   * @return status or null
   * @throws IOException IO Failure.
   */
  protected final FileStatus getFileStatusOrNull(Path path)
      throws IOException {
    try {
      return getFileStatus(path);
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  /**
   * Get a file status value or, if the path doesn't exist, return null.
   * @param path path
   * @return status or null
   * @throws IOException IO Failure.
   */
  protected final FileStatus getFileStatus(Path path) throws IOException {
    LOG.trace("getFileStatus('{}')", path);
    return trackDuration(getIOStatistics(), OP_GET_FILE_STATUS, () ->
        operations.getFileStatus(path));
  }

  /**
   * Delete a path.
   * @param path path
   * @param recursive recursive delete.
   * @return status or null
   * @throws IOException IO Failure.
   */
  protected final boolean delete(Path path, final boolean recursive)
      throws IOException {
    LOG.trace("delete('{}, {}')", path, recursive);
    return trackDuration(getIOStatistics(), OP_DELETE, () ->
        operations.delete(path, recursive));
  }

  /**
   * Create a directory.
   * @param path path
   * @param escalateFailure escalate "false" to PathIOE
   * @return true if the directory was created.
   * @throws IOException IO Failure.
   */
  protected final boolean mkdirs(Path path, boolean escalateFailure)
      throws IOException {
    LOG.trace("mkdirs('{}')", path);
    return trackDuration(getIOStatistics(), OP_MKDIRS, () -> {
      boolean success = operations.mkdirs(path);
      if (!success && escalateFailure) {
        throw new PathIOException(path.toUri().toString(),
            stageStatisticName + " : mkdirs() returned false");
      }
      return success;
    });

  }

  /**
   * List all directly files under a path.
   * Async implementations may under-report their durations.
   * @param path path
   * @return iterator over the results.
   * @throws IOException IO Failure.
   */
  protected final RemoteIterator<FileStatus> listStatusIterator(Path path)
      throws IOException {
    LOG.trace("listStatusIterator('{}')", path);
    return trackDuration(getIOStatistics(), OP_LIST_STATUS, () ->
        operations.listStatusIterator(path));
  }

  /**
   * Load a manifest file.
   * @param status source.
   * @return the manifest.
   * @throws IOException IO Failure.
   */
  protected final TaskManifest loadManifest(final FileStatus status)
      throws IOException {
    LOG.trace("loadManifest('{}')", status);
    return trackDuration(getIOStatistics(), OP_LOAD_MANIFEST, () ->
        operations.loadTaskManifest(status));
  }

  /**
   * List all the manifests in the job attempt dir.
   * @return a iterator of manifests.
   * @throws IOException IO Failure.
   */
  protected final RemoteIterator<FileStatus> listManifests()
      throws IOException {
    return RemoteIterators.filteringRemoteIterator(
        listStatusIterator(getJobAttemptDir()),
        st -> st.getPath().toUri().toString().endsWith(MANIFEST_SUFFIX));
  }

  /**
   * Create a directory -failing if it exists or if
   * mkdirs() failed.
   * @param operation operation for error reporting.
   * @param path path path to create.
   * @return the path.
   * @throws IOException failure
   * @throws PathIOException mkdirs failed.
   * @throws FileAlreadyExistsException destination exists.
   */
  protected final Path createNewDirectory(String operation,
      Path path) throws IOException {
    LOG.debug("{}: creating directory which must not exist {}",
        operation, path);
    final FileStatus status = getFileStatusOrNull(path);
    if (status == null) {
      mkdirs(path, true);
      return path;
    } else {
      String type = status.isDirectory() ? "directory" : "file";
      LOG.error("{}: {}: {} exists: {}", this, operation, type, status);
      throw new FileAlreadyExistsException(
          toString() + " " + operation + ": " + path + " exists and is a "
              + type);
    }
  }

  /**
   * Assert that a path is a directory which must exist.
   * @param operation operation for error reporting.
   * @param path path path to create.
   * @return the path
   * @throws IOException failure
   * @throws PathIOException mkdirs failed.
   * @throws FileAlreadyExistsException destination exists.
   */
  protected final Path directoryMustExist(String operation,
      Path path) throws IOException {
    final FileStatus status = getFileStatus(path);
    if (!status.isDirectory()) {
      throw new PathIOException(path.toString(),
          operation + ": "
              + path
              + "is not a directory; the status of the path is :" + status);
    }
    return path;
  }

  /**
   * Save a task manifest. This will be done by
   * writing to a temp path and then renaming.
   * If the destination path exists: Delete it.
   * @param manifestData the manifest/success file
   * @param tempPath temp path for the initial save
   * @param finalPath final path for rename.
   * @throws IOException failure to load/parse
   */
  protected final void save(AbstractManifestData manifestData,
      final Path tempPath,
      final Path finalPath) throws IOException {
    LOG.trace("save('{}, {}, {}')", manifestData, tempPath, finalPath);
    trackDurationOfInvocation(getIOStatistics(), OP_SAVE_TASK_MANIFEST, () ->
        operations.save(manifestData, tempPath, true));
    rename(tempPath, finalPath);
  }

  /**
   * Rename a file from source to dest; if the underlying FS API call
   * returned false that's escalated to an IOE.
   * @param source source file.
   * @param dest dest file
   * @throws IOException failure
   * @throws PathIOException if the rename() call returned false.
   */
  protected final void rename(final Path source, final Path dest)
      throws IOException {
    // delete the destination, always, knowing that it's a no-op if
    // the data isn't there. Skipping the change saves one round trip
    // to actually look for the file/object
    boolean deleted = delete(dest, true);
    // log the outcome in case of emergency diagnostics traces
    // being needed.
    LOG.debug("delete('{}') returned {}')", dest, deleted);

    // now do the rename.
    // the uprating of a false to PathIOE is done in the duration
    // tracker, so failures are counted.
    trackDurationOfInvocation(getIOStatistics(), OP_RENAME_FILE, () -> {
      boolean renamed = operations.renameFile(source, dest);
      if (!renamed) {
        // rename failed, and the FS isn't telling us why.
        // lets see what happened.
        final FileStatus sourceStatus = getFileStatusOrNull(source);
        final FileStatus deestStatus = getFileStatusOrNull(dest);
        LOG.error("rename failure from {} to {}", sourceStatus, deestStatus);
        throw new PathIOException(source.toString(),
            "Failed to rename "
                + source + " (" + sourceStatus + ")"
                + " to " + dest + " (" + deestStatus + ")");
      }
    });

  }

  /**
   * Job ID: never null.
   */
  protected final String getJobId() {
    return stageConfig.getJobId();
  }

  /**
   * Job ID: never null.
   */
  protected final int getJobAttemptNumber() {
    return stageConfig.getJobAttemptNumber();
  }

  /**
   * ID of the task.
   */
  protected final String getTaskId() {
    return stageConfig.getTaskId();
  }

  /**
   * Get the task ID; raise an NPE
   * if it is null.
   * @return a non-null task ID.
   */
  protected final String getRequiredTaskId() {
    return requireNonNull(getTaskId(),
        "No Task ID in stage config");
  }

  /**
   * ID of this specific attempt at a task.
   */
  protected final String getTaskAttemptId() {
    return stageConfig.getTaskAttemptId();
  }

  /**
   * Get the task attempt ID; raise an NPE
   * if it is null.
   * @return a non-null task attempt ID.
   */
  protected final String getRequiredTaskAttemptId() {
    return requireNonNull(getTaskAttemptId(),
        "No Task Attempt ID in stage config");
  }

  /**
   * Job attempt dir.
   */
  protected final Path getJobAttemptDir() {
    return stageConfig.getJobAttemptDir();
  }

  /**
   * Task attempt dir.
   */
  protected final Path getTaskAttemptDir() {
    return stageConfig.getTaskAttemptDir();
  }

  /**
   * Get the task attemptDir; raise an NPE
   * if it is null.
   * @return a non-null task attempt dir.
   */
  protected final Path getRequiredTaskAttemptDir() {
    return requireNonNull(getTaskAttemptDir(),
        "No Task Attempt Dir");
  }

  /**
   * Destination of job.
   */
  protected final Path getDestinationDir() {
    return stageConfig.getDestinationDir();
  }

  /**
   * Submitter for doing IO against the store other than
   * manifest processing.
   */
  protected final TaskPool.Submitter getIOProcessors() {
    return ioProcessors;
  }

  /**
   * Delete a directory, possibly suppressing exceptions.
   * @param dir directory.
   * @param suppressExceptions should exceptions be suppressed?
   * @throws IOException exceptions raised in delete if not suppressed.
   */
  protected void deleteDir(final Path dir,
      final Boolean suppressExceptions)
      throws IOException {
    try {
      delete(dir, true);
    } catch (IOException ex) {
      LOG.info("Error deleting {}: {}", dir, ex.toString());
      if (!suppressExceptions) {
        throw ex;
      }
    }
  }

  /**
   * Move dest/__temporary to trash using the jobID as the unique name.
   * @return false if it did not work.
   */
  protected boolean moveOutputTemporaryDirToTrash() throws IOException {
    return trackDuration(getIOStatistics(), OP_RENAME, () ->
        operations.moveToTrash(getJobId(),
            getStageConfig().getOutputTempSubDir()));
  }

}
