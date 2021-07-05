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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.util.functional.RemoteIterators;

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUCCESS_MARKER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Support for committer tests.
 */
public class ManifestCommitterTestSupport {

  private static final Logger LOG = LoggerFactory.getLogger(
      ManifestCommitterTestSupport.class);

  /**
   * Create a random Job ID using the fork ID as part of the number if
   * set in the current process.
   * @return fork ID string in a format parseable by Jobs
   */
  public static String randomJobId() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id", "0001");
    int l = testUniqueForkId.length();
    String trailingDigits = testUniqueForkId.substring(l - 4, l);
    try {
      int digitValue = Integer.valueOf(trailingDigits);
      return String.format("202170712%04d_%04d",
          (long) (Math.random() * 1000),
          digitValue);
    } catch (NumberFormatException e) {
      throw new RuntimeException("Failed to parse " + trailingDigits
          +" check the maven forkID settings", e);
    }
  }

  /**
   * Load a success file; fail if the file is empty/nonexistent.
   * @param fs filesystem
   * @param outputPath directory containing the success file.
   * @return the loaded file.
   * @throws IOException failure to find/load the file
   * @throws AssertionError file is 0-bytes long,
   */
  public static ManifestSuccessData loadSuccessFile(final FileSystem fs,
      final Path outputPath) throws IOException {
    Path success = new Path(outputPath, SUCCESS_MARKER);
    return ManifestSuccessData.load(fs, success);
  }

  /**
   * Load in the success data marker.
   * @param outputPath path of job
   * @param fs filesystem
   * @param origin origin (e.g. "teragen" for messages)
   * @param minimumFileCount minimum number of files to have been created
   * @param jobId job ID, only verified if non-empty
   * @return the success data
   * @throws IOException IO failure
   */
  public static ManifestSuccessData validateSuccessFile(final Path outputPath,
      final FileSystem fs,
      final String origin,
      final int minimumFileCount,
      final String jobId) throws IOException {
    ManifestSuccessData successData = loadSuccessFile(fs, outputPath);
    String commitDetails = successData.toString();
    LOG.info("Manifest {}\n{}", outputPath, commitDetails);
    LOG.info("Job IOStatistics: \n{}",
        ioStatisticsToPrettyString(successData.getIOStatistics()));
    LOG.info("Diagnostics\n{}",
        successData.dumpDiagnostics("  ", " = ", "\n"));
    assertThat(successData.getCommitter())
        .describedAs("Committer field in " + commitDetails)
        .isEqualTo(
            "org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitter");
    assertThat(successData.getFilenames())
        .describedAs("Files committed in " + commitDetails)
        .hasSizeGreaterThanOrEqualTo(minimumFileCount);
    if (StringUtils.isNotEmpty(jobId)) {
      assertThat(successData.getJobId())
          .describedAs("JobID in " + commitDetails)
          .isEqualTo(jobId);
    }
    return successData;
  }


  /**
   * List a directory/directory tree.
   * @param fileSystem FS
   * @param path path
   * @param recursive do a recursive listing?
   * @return the number of files found.
   * @throws IOException failure.
   */
  public static long lsR(FileSystem fileSystem, Path path, boolean recursive)
      throws Exception {
    if (path == null) {
      // surfaces when someone calls getParent() on something at the top
      // of the path
      LOG.info("Empty path");
      return 0;
    }
    return RemoteIterators.foreach(fileSystem.listFiles(path, recursive),
        (status) -> LOG.info("{}", status));
  }

  /**
   * Assert that a file or dir entry matches the given parameters.
   * Matching on paths, not strings, helps validate marshalling.
   * @param fileOrDir file or directory
   * @param src source path
   * @param dest dest path
   * @param l length
   */
  static void assertFileOrDirEntryMatch(
      final FileOrDirEntry fileOrDir,
      final Path src,
      final Path dest,
      final long l) {
    String entry = fileOrDir.toString();
    assertThat(fileOrDir.getSourcePath())
        .describedAs("Source path of " + entry)
        .isEqualTo(src);
    assertThat(fileOrDir.getDestPath())
        .describedAs("Dest path of " + entry)
        .isEqualTo(dest);
    assertThat(fileOrDir.getSize())
        .describedAs("Size of " + entry)
        .isEqualTo(l);
  }


  /**
   * Create a task attempt for a Job. This is based on the code
   * run in the MR AM, creating a task (0) for the job, then a task
   * attempt (0).
   * @param jobId job ID
   * @param jContext job context
   * @return the task attempt.
   */
/*
  public static TaskAttemptContext taskAttemptForJob(JobId jobId,
      JobContext jContext) {
    org.apache.hadoop.mapreduce.v2.api.records.TaskId taskID =
        MRBuilderUtils.newTaskId(jobId, 0,
            org.apache.hadoop.mapreduce.v2.api.records.TaskType.MAP);
    org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId attemptID =
        MRBuilderUtils.newTaskAttemptId(taskID, 0);
    return new TaskAttemptContextImpl(
        jContext.getConfiguration(),
        TypeConverter.fromYarn(attemptID));
  }
*/


  /**
   * Closeable which can be used to safely close writers in
   * a try-with-resources block..
   */
  public static class CloseWriter implements AutoCloseable {

    private final RecordWriter writer;

    private final TaskAttemptContext context;

    public CloseWriter(RecordWriter writer,
        TaskAttemptContext context) {
      this.writer = writer;
      this.context = context;
    }

    @Override
    public void close() {
      try {
        writer.close(context);
      } catch (IOException | InterruptedException e) {
        LOG.error("When closing {} on context {}",
            writer, context, e);
      }
    }
  }

  public static final String ATTEMPT_STRING =
      "attempt_%s_m_%06d_%d";

  /**
   * Creates a random JobID and then as many tasks
   * with the specific number of task attempts
   */
  public static class JobAndTaskIDsForTests {

    /** Job ID; will be created uniquely for each instance. */
    private final String jobId;

    /**
     * Store the details as strings; generate
     * IDs on demand.
     */
    private final String taskAttempts[][];

    /**
     * Constructor.
     * @param tasks number of tasks.
     * @param attempts number of attempts.
     */
    public JobAndTaskIDsForTests(int tasks, int attempts) {
      this(randomJobId(), tasks, attempts);
    }

    public JobAndTaskIDsForTests(final String jobId,
        int tasks, int attempts) {
      this.jobId = jobId;
      this.taskAttempts = new String[tasks][attempts];
      for (int i = 0; i < tasks; i++) {
        for (int j = 0; j < attempts; j++) {
          String a = String.format(ATTEMPT_STRING,
              jobId, i, j);
          this.taskAttempts[i][j] = a;
        }
      }
    }

    /**
     * Get the job ID.
     * @return job ID string.
     */
    public String getJobId() {
      return jobId;
    }

    /**
     * Get the job ID as the MR type.
     * @return job ID type.
     */
    public JobID getJobIdType() {
      return getTaskIdType(0).getJobID();
    }

    /**
     * Get a task attempt ID.
     * @param task task index
     * @param attempt attempt number.
     * @return the task attempt.
     */
    public String getTaskAttempt(int task, int attempt) {
      return taskAttempts[task][attempt];
    }

    /**
     * Get task attempt ID as the MR type.
     * @param task task index
     * @param attempt attempt number.
     * @return the task attempt type
     */
    public TaskAttemptID getTaskAttemptIdType(int task, int attempt) {
      return TaskAttemptID.forName(getTaskAttempt(task, attempt));
    }

    /**
     * Get task ID as the MR type.
     * @param task task index
     * @return the task ID type
     */
    public TaskID getTaskIdType(int task) {
      return TaskAttemptID.forName(getTaskAttempt(task, 0)).getTaskID();
    }

    /**
     * Get task ID as a string.
     * @param task task index
     * @return the task ID
     */
    public String getTaskId(int task) {
      return getTaskIdType(task).toString();
    }

  }
}
