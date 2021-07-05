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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import static org.apache.hadoop.fs.contract.ContractTestUtils.rm;
import static org.apache.hadoop.fs.contract.ContractTestUtils.verifyPathExists;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_ID_SOURCE_MAPREDUCE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_COMMITTER_CLASSNAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.PRINCIPAL;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterSupport.manifestPathForTask;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test IO through the stages.
 * This mimics the workflow of a job with two tasks,
 * the first task has two attempts where the second attempt
 * is committed after the first attempt (simulating the
 * failure-during-task-commit which the v2 algorithm cannot
 * handle).
 *
 * The test is ordered and the output dir is not cleaned up
 * after each test case.
 * The last test case MUST perform the cleanup.
 * The
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestJobThroughManifestCommitter
    extends AbstractManifestCommitterTest {

  private ManifestCommitterSupport.AttemptDirectories dirs;

  private Path destDir;

  /**
   * To ensure that the local FS has a shared root path, this is static.
   */
  private static Path SHARED_TEST_ROOT = null;

  /**
   * Job ID.
   */
  private String jobId;

  /**
   * Task 0 attempt 0 ID.
   */
  private String taskAttempt00;

  /**
   * Task 0 attempt 1 ID.
   */
  private String taskAttempt01;

  /**
   * Task 1 attempt 0 ID.
   */
  private String taskAttempt10;

  /**
   * Task 1 attempt 1 ID.
   */
  private String taskAttempt11;

  /**
   * Stage config for TA00.
   */
  private StageConfig ta00Config;

  /**
   * Stage config for TA01.
   */
  private StageConfig ta01Config;

  /**
   * Stage config for TA10.
   */
  private StageConfig ta10Config;

  /**
   * Stage config for TA11.
   */
  private StageConfig ta11Config;

  /**
   * Job config has no task attempt information.
   */
  private StageConfig jobStageConfig;

  @Override
  public void setup() throws Exception {
    super.setup();
    taskAttempt00 = TASK_IDS.getTaskAttempt(TASK0, TA0);
    taskAttempt01 = TASK_IDS.getTaskAttempt(TASK0, TA1);
    taskAttempt10 = TASK_IDS.getTaskAttempt(TASK1, TA0);
    taskAttempt11 = TASK_IDS.getTaskAttempt(TASK1, TA1);
    setSharedPath(path("TestJobThroughManifestCommitter"));
    // add a dir with a space in.
    destDir = new Path(SHARED_TEST_ROOT, "out put");
    jobId = TASK_IDS.getJobId();
    // then the specific path underneath that for the attempt.
    dirs = new ManifestCommitterSupport.AttemptDirectories(destDir,
        jobId, 1);

    // config for job attempt 1, task 00
    jobStageConfig = createStageConfigForJob(JOB1, destDir).build();
    ta00Config = createStageConfig(JOB1, TASK0, TA0, destDir).build();
    ta01Config = createStageConfig(JOB1, TASK0, TA1, destDir).build();
    ta10Config = createStageConfig(JOB1, TASK1, TA0, destDir).build();
    ta11Config = createStageConfig(JOB1, TASK1, TA1, destDir).build();
  }

  /**
   * Test dir deletion is removed from test case teardown so the
   * subsequent tests see the output.
   * @throws IOException failure
   */
  @Override
  protected void deleteTestDirInTeardown() throws IOException {
    /* no-op */
  }

  /**
   * Invoke this to clean up the test directories.
   */
  private void deleteSharedTestRoot() throws IOException {
    rm(getFileSystem(), SHARED_TEST_ROOT, true, false);
  }

  /**
   * Set the shared test root if not already set.
   * @param path path to set.
   * @return true if the path was set
   */
  private static synchronized boolean setSharedPath(final Path path) {
    if (SHARED_TEST_ROOT == null) {
      // set this as needed
      LOG.info("Set shared path to {}", path);
      SHARED_TEST_ROOT = path;
      return true;
    }
    return false;
  }

  @Test
  public void test_0000_setupTestDir() throws Throwable {
    describe("always ensure directory setup is empty");
    deleteSharedTestRoot();
  }

  @Test
  public void test_0050_setupTaskFailsNoJob() throws Throwable {
    describe("Set up a task; job must have been set up first");
    intercept(FileNotFoundException.class, "", () ->
        new SetupTaskStage(ta00Config).apply(""));
  }

  @Test
  public void test_0100_setupJobStage() throws Throwable {
    describe("Set up a job");
    verifyPath("Job attempt dir",
        dirs.getJobAttemptDir(),
        new SetupJobStage(jobStageConfig).apply(true));
  }

  @Test
  public void test_0110_setupJobOnlyAllowedOnce() throws Throwable {
    describe("a second creation of a job attempt must fail");
    intercept(FileAlreadyExistsException.class, "", () ->
        new SetupJobStage(jobStageConfig).apply(true));
    // job is still there
    assertPathExists("Job attempt dir", dirs.getJobAttemptDir());
  }

  @Test
  public void test_0120_setupJobNewAttemptNumber() throws Throwable {
    describe("Creating a new job attempt is supported");
    Path path = pathMustExist("Job attempt 2 dir",
        new SetupJobStage(createStageConfig(2, -1, 0, destDir))
            .apply(false));
    Assertions.assertThat(path)
        .describedAs("Stage created path")
        .isNotEqualTo(dirs.getJobAttemptDir());
  }

  @Test
  public void test_0200_setupTask00() throws Throwable {
    describe("Set up a task; job must have been set up first");
    verifyPath("Task attempt 00",
        dirs.getTaskAttemptPath(taskAttempt00),
        new SetupTaskStage(ta00Config).apply("first"));
  }

  @Test
  public void test_0210_setupTask00OnlyAllowedOnce() throws Throwable {
    describe("Second attempt to set up task00 must fail.");
    intercept(FileAlreadyExistsException.class, "second", () ->
        new SetupTaskStage(ta00Config).apply("second"));
  }

  @Test
  public void test_0220_setupTask01() throws Throwable {
    describe("Setup task attempt 01");
    verifyPath("Task attempt 01",
        dirs.getTaskAttemptPath(taskAttempt01),
        new SetupTaskStage(ta01Config)
            .apply("01"));
  }

  @Test
  public void test_0230_setupTask10() throws Throwable {
    describe("Setup task attempt 10");
    verifyPath("Task attempt 10",
        dirs.getTaskAttemptPath(taskAttempt10),
        new SetupTaskStage(ta10Config)
            .apply("10"));
  }

  /**
   * Setup then abort task 11 before creating any files;
   * verify that commit fails before creating a manifest file.
   */
  @Test
  public void test_0240_setupThenAbortTask11() throws Throwable {
    describe("Setup then abort task attempt 11");
    Path ta11Path = new SetupTaskStage(ta11Config).apply("11");
    Path deletedDir = new AbortTaskStage(ta11Config).apply(false);
    Assertions.assertThat(ta11Path)
        .isEqualTo(deletedDir);
    assertPathDoesNotExist("aborted directory", ta11Path);
    // execute will fail as there's no dir to list.
    intercept(FileNotFoundException.class, () ->
        new CommitTaskStage(ta11Config).apply("task11"));
    assertPathDoesNotExist("task manifest",
        manifestPathForTask(dirs.getJobAttemptDir(),
            TASK_IDS.getTaskId(TASK1)));
  }

  /**
   * Execute TA01 by generating a lot of files in its directory
   * then committing the task attempt.
   * The manifest at the task path (i.e. the record of which attempt's
   * output is to be used) MUST now have been generated by this TA.
   */
  @Test
  public void test_0300_executeTask00() throws Throwable {
    describe("Create the files for Task 00, then commit the task");
    List<Path> files = createFiles(dirs.getTaskAttemptPath(taskAttempt00),
        "part-00", getSubmitter().getPool(),
        DEPTH, WIDTH, FILES_PER_DIRECTORY);
    // saves the task manifest to the job dir
    Pair<Path, TaskManifest> pair = new CommitTaskStage(ta00Config).apply(
        "task");
    Path manifestPath = verifyPathExists(getFileSystem(), "manifest",
        pair.getLeft()).getPath();

    TaskManifest manifest = pair.getRight();
    manifest.validate();
    // clear the IOStats to reduce the size of the printed JSON.
    manifest.setIOStatistics(null);
    LOG.info("Task Manifest {}", manifest.toJson());
    verifyManifestTaskAttemptID(manifest, taskAttempt00);

    // validate the manifest
    verifyManifestFilesMatch(manifest, files);
  }

  /**
   * Execute TA01 by generating a lot of files in its directory
   * then committing the task attempt.
   * The manifest at the task path (i.e. the record of which attempt's
   * output is to be used) MUST now have been generated by this TA.
   * Any existing manifest will have been overwritten.
   */
  @Test
  public void test_0310_executeTask01() throws Throwable {
    describe("Create the files for Task 01, then commit the task");
    List<Path> files = createFiles(dirs.getTaskAttemptPath(taskAttempt01),
        "part-00", getSubmitter().getPool(),
        DEPTH, WIDTH, FILES_PER_DIRECTORY);
    // saves the task manifest to the job dir
    Pair<Path, TaskManifest> pair = new CommitTaskStage(ta01Config).apply(
        "task");
    Path manifestPath = verifyPathExists(getFileSystem(), "manifest",
        pair.getLeft()).getPath();

    // load the manifest from the FS, not the return value,
    // so we can verify that last task to commit wins.
    TaskManifest manifest = TaskManifest.load(getFileSystem(), manifestPath);
    manifest.validate();
    // clear the IOStats to reduce the size of the printed JSON.
    manifest.setIOStatistics(null);
    LOG.info("Task Manifest {}", manifest.toJson());

    verifyManifestTaskAttemptID(manifest, taskAttempt01);
    verifyManifestFilesMatch(manifest, files);

  }

  /**
   * Second task writes to more directories, but fewer files per dir.
   * This ensures that there will dirs here which aren't in the first
   * attempt.
   */
  @Test
  public void test_0320_executeTask10() throws Throwable {
    describe("Create the files for Task 10, then commit the task");
    List<Path> files = createFiles(
        dirs.getTaskAttemptPath(ta10Config.getTaskAttemptId()),
        "part-01", getSubmitter().getPool(),
        DEPTH, WIDTH + 1, FILES_PER_DIRECTORY - 1);
    // saves the task manifest to the job dir
    Pair<Path, TaskManifest> pair = new CommitTaskStage(ta10Config).apply(
        "task");
    TaskManifest manifest = pair.getRight();
    verifyManifestTaskAttemptID(manifest, taskAttempt10);
    // validate the manifest
    verifyManifestFilesMatch(manifest, files);
  }

  @Test
  public void test_0340_setupThenAbortTask11() throws Throwable {
    describe("Setup then abort task attempt 11");
    Path ta11Path = new SetupTaskStage(ta11Config).apply("11");
    List<Path> files = createFiles(
        ta11Path,
        "part-01", getSubmitter().getPool(),
        2, 1, 1);

    Path deletedDir = new AbortTaskStage(ta11Config).apply(false);
    assertPathDoesNotExist("aborted directory", ta11Path);
    // execute will fail as there's no dir to list.
    intercept(FileNotFoundException.class, () ->
        new CommitTaskStage(ta11Config).apply("task11"));

    // and the manifest MUST be unchanged from the previous stage
    Path manifestPathForTask1 = manifestPathForTask(dirs.getJobAttemptDir(),
        TASK_IDS.getTaskId(TASK1));
    verifyManifestTaskAttemptID(
        TaskManifest.load(getFileSystem(), manifestPathForTask1),
        taskAttempt10);

  }

  /**
   * Load all the committed manifests, which must be TA01 (last of
   * task 0 to commit) and TA10.
   */
  @Test
  public void test_0400_loadManifests() throws Throwable {
    describe("Load all manifests; committed must be TA01 and TA10");
    Pair<LoadManifestsStage.SummaryInfo, List<TaskManifest>> pair
        = new LoadManifestsStage(jobStageConfig).apply(true);
    String summary = pair.getLeft().toString();
    LOG.info("Manifest summary {}", summary);
    List<TaskManifest> manifests = pair.getRight();
    Assertions.assertThat(manifests)
        .describedAs("Loaded manifests in %s", summary)
        .hasSize(2);
    Map<String, TaskManifest> manifestMap = toMap(manifests);
    TaskManifest manifest01 = verifyManifestTaskAttemptID(
        manifestMap.get(taskAttempt01), taskAttempt01);
    TaskManifest manifest10 = verifyManifestTaskAttemptID(
        manifestMap.get(taskAttempt10), taskAttempt10);
  }

  @Test
  public void test_0410_commitJob() throws Throwable {
    describe("Commit the job");
    CommitJobStage stage = new CommitJobStage(jobStageConfig);
    stage.apply(Pair.of(true, true));
  }

  /**
   * Validate that the job output is good by invoking the
   * {@link ValidateRenamedFilesStage} stage to
   * validate all the manifests.
   */
  @Test
  public void test_0420_validateJob() throws Throwable {
    describe("Validate the output of the job through the validation"
        + " stage");

    // load in the success data.
    ManifestSuccessData successData = ManifestSuccessData.load(
        getFileSystem(),
        ta00Config.getJobSuccessMarkerPath());
    String json = successData.toJson();
    LOG.info("Success data is {}", json);
    Assertions.assertThat(successData)
        .describedAs("Manifest " + json)
        .returns(NetUtils.getLocalHostname(),
            ManifestSuccessData::getHostname)
        .returns(MANIFEST_COMMITTER_CLASSNAME,
            ManifestSuccessData::getCommitter)
        .returns(jobId,
            ManifestSuccessData::getJobId)
        .returns(JOB_ID_SOURCE_MAPREDUCE,
            ManifestSuccessData::getJobIdSource);
    String userName = UserGroupInformation.getCurrentUser()
        .getShortUserName();
    Assertions.assertThat(successData.getDiagnostics())
        .hasEntrySatisfying(PRINCIPAL, v ->
            v.equals(userName));

    // load manifests stage will load all the task manifests again
    List<TaskManifest> manifests = new LoadManifestsStage(jobStageConfig)
        .apply(true)
        .getRight();
    // Now verify their files exist, returning the list of renamed files.
    List<String> committedFiles = new ValidateRenamedFilesStage(jobStageConfig)
        .apply(manifests)
        .stream().map(FileOrDirEntry::getDest)
        .collect(Collectors.toList());

    // verify that the list of committed files also matches
    // that in the _SUCCESS file
    // note: there's a limit to the #of files in the SUCCESS file
    // to stop writing it slowing down jobs; therefore we don't
    // make a simple "all must match check
    Assertions.assertThat(committedFiles)
        .containsAll(successData.getFilenames());

    // now patch one of the manifest files by editing an entry
    FileOrDirEntry entry = manifests.get(0).getFilesToCommit().get(0);
    // no longer exists.
    String oldName = entry.getDest();
    String newName = oldName + ".missing";
    entry.setDest(newName);

    // validation will now fail
    intercept(FileNotFoundException.class, ".missing", () ->
        new ValidateRenamedFilesStage(jobStageConfig)
            .apply(manifests));

    // restore the name, but change the size
    entry.setDest(oldName);
    entry.setSize(128_000_000);
    intercept(PathIOException.class, () ->
        new ValidateRenamedFilesStage(jobStageConfig)
            .apply(manifests));
  }

  @Test
  public void test_0900_cleanupJob() throws Throwable {
    describe("Cleanup job");
    CleanupJobStage.CleanupResult result = new CleanupJobStage(
        jobStageConfig).apply(
        new CleanupJobStage.Options(true, true, false, false));
    Assertions.assertThat(result)
        .matches(r -> !r.wasSkipped(), "was skipped")
        .matches(r -> !r.wasRenamed(), "was renamed")
        .extracting(CleanupJobStage.CleanupResult::getDeleteCalls)
        .isEqualTo(1);
    assertPathDoesNotExist("Job attempt dir", result.getDirectory());

    // not an error if we retry and the dir isn't there
    new CleanupJobStage(jobStageConfig).apply(
        new CleanupJobStage.Options(true, true, false, false));
  }

  @Test
  public void test_9999_cleanupTestDir() throws Throwable {
    deleteSharedTestRoot();
  }

}
