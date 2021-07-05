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

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileOrDirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;

import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_RENAME_FILE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.UnreliableStoreOperations.SIMULATED_FAILURE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test rename files with fault injection.
 * This is done with a stub FS and no real file IO;
 */
public class TestRenameFilesStage extends AbstractManifestCommitterTest {

  public static final String RENAME_FAILURES = OP_RENAME_FILE + ".failures";
  /**
   * Fault Injection.
   */
  private UnreliableStoreOperations failures;

  @Override
  public void setup() throws Exception {
    super.setup();
    failures
        = new UnreliableStoreOperations(new StubStoreOperations());
    setStoreOperations(failures);
  }

  @Test
  public void testRenameFailure() throws Throwable {

    // destination directory.
    Path destDir = methodPath();
    StageConfig jobStageConfig = createStageConfigForJob(JOB1, destDir);
    Path jobAttemptTaskSubDir = jobStageConfig.getJobAttemptTaskSubDir();

    // create a manifest with a lot of files, but for
    // which one of whose renames will fail
    TaskManifest manifest = new TaskManifest();
    Path file500;
    int files = 1000;
    for (int i = 0; i < files; i++) {
      String name = String.format("file%04d", i);
      Path src = new Path(jobAttemptTaskSubDir, name);
      Path dest = new Path(destDir, name);
      manifest.addFileToCommit(new FileOrDirEntry(src, dest, 0));
      if (i == 500) {
        // add file #500 to the failure list, and only that file.
        file500 = src;
        failures.addRenameSourceFilesToFail(src);
      }
    }

    List<TaskManifest> manifests = new ArrayList<>();
    manifests.add(manifest);

    // rename MUST fail
    expectRenameFailure(new RenameFilesStage(jobStageConfig), manifests, files,
        SIMULATED_FAILURE);

    // switch to rename returning false.; again, this must
    // be escalated to a failure.
    failures.setRenameToFailWithException(false);
    expectRenameFailure(new RenameFilesStage(jobStageConfig), manifests, files,
        "");
  }

  private void expectRenameFailure(RenameFilesStage stage,
      List<TaskManifest> manifests,
      int files, String errorText) throws Exception {
    ProgressCounter progressCounter = getProgressCounter();
    progressCounter.reset();
    IOStatisticsStore iostatistics = stage.getIOStatistics();
    long failures0 = iostatistics.counters().get(RENAME_FAILURES);

    // rename MUST raise an exception.
    intercept(PathIOException.class, errorText, () ->
        stage.apply(manifests));

    // the IOStatistics record the rename as a failure.
    assertThatStatisticCounter(iostatistics, RENAME_FAILURES)
        .isEqualTo(failures0 + 1);

    // count of files committed MUST be less than expected.
    Assertions.assertThat(stage.getFilesCommitted().size())
        .describedAs("Files Committed by stage")
        .isGreaterThan(0)
        .isLessThan(files);

    // the progress counter will show that the rename did NOT complete,
    // that is: work stopped partway through.

    Assertions.assertThat(progressCounter.value())
        .describedAs("Progress counter %s", progressCounter)
        .isGreaterThan(0)
        .isLessThan(files);


  }
}
