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

import org.apache.hadoop.classification.InterfaceAudience;

import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_CONTINUE_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_GET_FILE_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_DIRECTORY;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_LIST_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASKS_COMPLETED;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_CREATE_DIRECTORIES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_CREATE_ONE_DIRECTORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DIRECTORY_SCAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_JOB_COMMITTED_BYTES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_JOB_COMMITTED_FILES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_LOAD_ALL_MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_LOAD_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_RENAME_FILE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_SAVE_TASK_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CLEANUP;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CREATE_TARGET_DIRS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_LOAD_MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_RENAME_FILES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_SAVE_SUCCESS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_SETUP;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_VALIDATE_OUTPUT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_ABORT_TASK;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SAVE_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SCAN_DIRECTORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_TASK_SETUP;

/**
 * Constants internal and external for the manifest committer.
 */

@InterfaceAudience.Private
public final class ManifestCommitterConstants {

  private ManifestCommitterConstants() {
  }

  /**
   * Suffix to use in manifest files in the job attempt dir.
   * Value: {@value}.
   */
  public static final String MANIFEST_SUFFIX = "-manifest.json";

  /**
   * Suffix to use for temp files before renaming them.
   * Value: {@value}.
   */
  public static final String TMP_SUFFIX = ".tmp";

  /**
   * Initial number of all app attempts.
   * This is fixed in YARN; for Spark jobs the
   * same number "0" is used.
   */
  public static final int INITIAL_APP_ATTEMPT_ID = 0;

  /**
   * Format string for building a job dir.
   * Value: {@value}.
   */
  public static final String JOB_DIR_FORMAT_STR =
      "manifest_%s";

  /**
   * Format string for building a job attempt dir.
   * This uses the job attempt number so previous versions
   * can be found trivially.
   * Value: {@value}.
   */
  public static final String JOB_ATTEMPT_DIR_FORMAT_STR =
      "%d";


  /**
   * Durations.
   */
  public static final String[] DURATION_STATISTICS = {

      /* Job stages. */
      OP_STAGE_JOB_CLEANUP,
      OP_STAGE_JOB_COMMIT,
      OP_STAGE_JOB_CREATE_TARGET_DIRS,
      OP_STAGE_JOB_LOAD_MANIFESTS,
      OP_STAGE_JOB_RENAME_FILES,
      OP_STAGE_JOB_SAVE_SUCCESS,
      OP_STAGE_JOB_SETUP,
      OP_STAGE_JOB_VALIDATE_OUTPUT,

      /* Task stages. */

      OP_STAGE_TASK_ABORT_TASK,
      OP_STAGE_TASK_COMMIT,
      OP_STAGE_TASK_SAVE_MANIFEST,
      OP_STAGE_TASK_SCAN_DIRECTORY,
      OP_STAGE_TASK_SETUP,


      /* Lower level store/fs operations. */
      OP_CREATE_DIRECTORIES,
      OP_CREATE_ONE_DIRECTORY,
      OP_DIRECTORY_SCAN,
      OP_DELETE,
      OP_GET_FILE_STATUS,
      OP_IS_DIRECTORY,
      OP_LIST_STATUS,
      OP_LOAD_MANIFEST,
      OP_LOAD_ALL_MANIFESTS,
      OP_MKDIRS,
      OP_RENAME_FILE,
      OP_SAVE_TASK_MANIFEST,

      OBJECT_LIST_REQUEST,
      OBJECT_CONTINUE_LIST_REQUEST
  };

  /**
   * Counters.
   */
  public static final String[] COUNTER_STATISTICS = {
      COMMITTER_TASKS_COMPLETED,

      OP_JOB_COMMITTED_BYTES,
      OP_JOB_COMMITTED_FILES
  };

  /**
   * Committer classname as recorded in the committer _SUCCESS file.
   */
  public static final String MANIFEST_COMMITTER_CLASSNAME =
      "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitter";

  /**
   * Marker file to create on success: {@value}.
   */
  public static final String SUCCESS_MARKER = "_SUCCESS";

  /** Default job marker option: {@value}. */
  public static final boolean DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER = true;


  /**
   * The limit to the number of committed objects tracked during
   * job commits and saved to the _SUCCESS file.
   */
  public static final int SUCCESS_MARKER_FILE_LIMIT = 100;

  /**
   * The UUID for jobs: {@value}.
   * This was historically created in Spark 1.x's SQL queries, but "went away".
   * It has been restored in recent spark releases.
   * If found: it is used instead of the MR job attempt ID.
   */
  public static final String SPARK_WRITE_UUID =
      "spark.sql.sources.writeJobUUID";

  /**
   * String to use as source of the job ID.
   * This SHOULD be kept in sync with that of
   * {@code AbstractS3ACommitter.JobUUIDSource}.
   */
  public static final String JOB_ID_SOURCE_MAPREDUCE =
      "JobID";

  /**
   * Prefix to use for config options: {@value }.
   */
  public static final String OPT_PREFIX = "mapreduce.manifest.committer.";

  /**
   * Threads to use for IO.
   */
  public static final String OPT_IO_PROCESSORS = OPT_PREFIX + "io.thread.count";

  /**
   * Default value:  {@value }.
   */
  public static final int OPT_IO_PROCESSORS_DEFAULT = 32;

  /**
   * Should the output be validated?
   */
  public static final String OPT_VALIDATE_OUTPUT = OPT_PREFIX
      + "validate.output";

  /**
   * Default value:  {@value }.
   */
  public static final boolean OPT_VALIDATE_OUTPUT_DEFAULT = false;

  /**
   * rather than delete in cleanup, should the working directory
   * be moved to the trash directory?
   * Potentially faster on some stores.
   */
  public static final String OPT_CLEANUP_MOVE_TO_TRASH =
      OPT_PREFIX +"cleanup.move.to.trash";

  /**
   * Default value:  {@value }.
   */
  public static final boolean OPT_CLEANUP_MOVE_TO_TRASH_DEFAULT =
      false;

  /**
   * Should dir cleanup do parallel deletion of task attempt dirs
   * before trying to delete the toplevel dirs.
   * For GCS this may deliver speedup, while on ABFS it may avoid
   * timeouts in certain deployments.
   */
  public static final String OPT_CLEANUP_PARALLEL_ATTEMPT_DIRS =
      OPT_PREFIX +"cleanup.parallel.delete.attempt.directories";

  /**
   * Default value:  {@value}.
   */
  public static final boolean OPT_CLEANUP_PARALLEL_ATTEMPT_DIRS_DEFAULT =
      true;

  /**
   * Name of the factory: {@value}.
   */
  public static final String MANIFEST_COMMITTER_FACTORY
      = "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory";


  /**
   * Attribute added to diagnostics in _SUCCESS file.
   */
  public static final String PRINCIPAL = "principal";

  /**
   * Error string from ABFS connector on timeout.
   */
  public static final String OPERATION_TIMED_OUT = "OperationTimedOut";

}
