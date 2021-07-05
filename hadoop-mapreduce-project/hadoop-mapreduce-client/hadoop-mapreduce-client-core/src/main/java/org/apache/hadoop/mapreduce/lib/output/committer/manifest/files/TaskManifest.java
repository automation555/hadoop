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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.util.JsonSerialization;

/**
 * This is the manifest of files which were created by
 * this task attempt.
 * Although based on the same design as the S3A PendingSet manifest,
 * there is no expectation that they should be
 */
@SuppressWarnings("unused")
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskManifest extends AbstractManifestData<TaskManifest> implements
    IOStatisticsSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(TaskManifest.class);

  /**
   * Job ID; constant over multiple attempts.
   */
  @JsonProperty("jobId")
  private String jobId;

  /**
   * Number of the job attempt; starts at zero.
   */
  @JsonProperty("jobAttemptNumber")
  private int jobAttemptNumber;

  /**
   * Task Attempt ID.
   */
  @JsonProperty("taskID")
  private String taskID;

  /**
   * Task Attempt ID.
   */
  @JsonProperty("taskAttemptID")
  private String taskAttemptID;

  @JsonProperty("taskAttemptDir")
  private String taskAttemptDir;

  /**
   * The list of files to commit from this task attempt, including
   * precalculated destination and size.
   */
  @JsonProperty("filesToCommit")
  private final List<FileOrDirEntry> filesToCommit = new ArrayList<>();

  /**
   * The list of directories needed by this task attempt, both
   * source and destination.
   * All these directories must exist in the destination before any of
   * the files can be renamed there.
   */
  @JsonProperty("directoriesToCreate")
  private final List<FileOrDirEntry> directoriesToCreate = new ArrayList<>();

  /**
   * Any custom extra data committers may choose to add.
   */
  private final Map<String, String> extraData = new HashMap<>(0);

  /**
   * IOStatistics.
   */
  @JsonProperty("iostatistics")
  private IOStatisticsSnapshot iostatistics = new IOStatisticsSnapshot();

  /**
   * Empty constructor; will be used by jackson as well as in application
   * code.
   */
  public TaskManifest() {
  }

  @Override
  public IOStatisticsSnapshot getIOStatistics() {
    return iostatistics;
  }

  public void setIOStatistics(
      @Nullable final IOStatisticsSnapshot ioStatistics) {
    this.iostatistics = ioStatistics;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(final String jobId) {
    this.jobId = jobId;
  }

  public int getJobAttemptNumber() {
    return jobAttemptNumber;
  }

  public void setJobAttemptNumber(final int jobAttemptNumber) {
    this.jobAttemptNumber = jobAttemptNumber;
  }

  public String getTaskID() {
    return taskID;
  }

  public void setTaskID(final String taskID) {
    this.taskID = taskID;
  }

  public String getTaskAttemptID() {
    return taskAttemptID;
  }

  public void setTaskAttemptID(final String taskAttemptID) {
    this.taskAttemptID = taskAttemptID;
  }

  public String getTaskAttemptDir() {
    return taskAttemptDir;
  }

  public void setTaskAttemptDir(final String taskAttemptDir) {
    this.taskAttemptDir = taskAttemptDir;
  }

  /**
   * Add a file to the list of files to commit.
   * @param entry entry  to add
   */
  public void addFileToCommit(FileOrDirEntry entry) {
    filesToCommit.add(entry);
  }

  public List<FileOrDirEntry> getFilesToCommit() {
    return filesToCommit;
  }

  /**
   * Calculate the total amout of data which will be committed.
   * @return the sum of sizes of all files to commit.
   */
  @JsonIgnore
  public long getTotalFileSize() {
    return filesToCommit.stream().collect(
        Collectors.summingLong(FileOrDirEntry::getSize));
  }

  public List<FileOrDirEntry> getDirectoriesToCreate() {
    return directoriesToCreate;
  }

  /**
   * Add a directory to the list of directories to create.
   * @param entry entry  to add
   */
  public void addDirectory(FileOrDirEntry entry) {
    directoriesToCreate.add(entry);
  }

  public Map<String, String> getExtraData() {
    return extraData;
  }

  @Override
  public byte[] toBytes() throws IOException {
    return serializer().toBytes(this);
  }

  /**
   * To JSON.
   * @return json string value.
   * @throws IOException failure
   */
  public String toJson() throws IOException {
    return serializer().toJson(this);
  }

  @Override
  public void save(FileSystem fs, Path path, boolean overwrite)
      throws IOException {
    serializer().save(fs, path, this, overwrite);
  }

  /**
   * Validate the data: those fields which must be non empty, must be set.
   * @throws IOException if the data is invalid
   */
  public void validate() throws IOException {
    // verify(version == VERSION, "Wrong version: %s", version);
    validateCollectionClass(extraData.keySet(), String.class);
    validateCollectionClass(extraData.values(), String.class);
    Set<String> destinations = new HashSet<>(filesToCommit.size());
    validateCollectionClass(filesToCommit, FileOrDirEntry.class);
    for (FileOrDirEntry c : filesToCommit) {
      c.validate();
      verify(!destinations.contains(c.getDest()),
          "Destination %s is written to by more than one pending commit",
          c.getDest());
      destinations.add(c.getDest());
    }
  }

  /**
   * Get a JSON serializer for this class.
   * @return a serializer.
   */
  @Override
  public JsonSerialization<TaskManifest> createSerializer() {
    return serializer();
  }

  /**
   * Create a JSON serializer for this class.
   * @return a serializer.
   */
  public static JsonSerialization<TaskManifest> serializer() {
    return new JsonSerialization<>(TaskManifest.class, false, true);
  }


  /**
   * Load an instance from a file, then validate it.
   * @param fs filesystem
   * @param path path
   * @return the loaded instance
   * @throws IOException IO failure/the data is invalid
   */
  public static TaskManifest load(FileSystem fs, Path path)
      throws IOException {
    LOG.debug("Reading Manifest in file {}", path);
    TaskManifest instance = serializer().load(fs, path);
    instance.validate();
    return instance;
  }

  /**
   * Load an instance from a file, then validate it.
   * If loading through a listing; use this API so that filestatus
   * hints can be used.
   * @param fs filesystem
   * @param status status of file to load
   * @return the loaded instance
   * @throws IOException IO failure
   * @throws IOException IO failure/the data is invalid
   */
  public static TaskManifest load(FileSystem fs, FileStatus status)
      throws IOException {
    return load(fs, status.getPath());
  }

}
