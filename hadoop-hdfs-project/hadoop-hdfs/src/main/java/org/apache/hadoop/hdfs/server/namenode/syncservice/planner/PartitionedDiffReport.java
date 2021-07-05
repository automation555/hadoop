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
package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceFileFilter;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.CREATE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.DELETE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.MODIFY;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.RENAME;

public class PartitionedDiffReport {

  private static final Comparator<? super DiffReportEntry>
      reverseSourceNameOrder = (Comparator<DiffReportEntry>)
      (o1, o2) -> {
        String sourcePath1 = DFSUtil.bytes2String(o1.getSourcePath());
        String sourcePath2 = DFSUtil.bytes2String(o2.getSourcePath());
        return sourcePath2.compareTo(sourcePath1);
      };
  private List<RenameEntryWithTemporaryName> renames;
  private List<TranslatedEntry> deletes;
  private List<TranslatedEntry> modifies;
  private List<TranslatedEntry> creates;
  private List<DiffReportEntry> createsFromRenames;

  @VisibleForTesting
  PartitionedDiffReport(List<RenameEntryWithTemporaryName> renames,
      List<TranslatedEntry> deletes, List<TranslatedEntry> modifies,
      List<TranslatedEntry> creates, List<DiffReportEntry> createsFromRenames) {
    this.renames = renames;
    this.deletes = deletes;
    this.modifies = modifies;
    this.creates = creates;
    this.createsFromRenames = createsFromRenames;
  }

  public static ResultingOperation determineResultingOperation(
      DiffReportEntry diffReportEntry,
      SyncServiceFileFilter syncServiceFileFilter) {
    boolean isSourceExcluded = syncServiceFileFilter.isExcluded(
        new File(DFSUtil.bytes2String(diffReportEntry.getSourcePath())));
    boolean isTargetExcluded = syncServiceFileFilter.isExcluded(
        new File(DFSUtil.bytes2String(diffReportEntry.getTargetPath())));

    if (isSourceExcluded && isTargetExcluded) {
      return ResultingOperation.NOOP;
    } else if (isSourceExcluded && !isTargetExcluded) {
      return ResultingOperation.CREATE;
    } else if (!isSourceExcluded && isTargetExcluded) {
      return ResultingOperation.DELETE;
    } else {
      return ResultingOperation.RENAME;
    }
  }

  public static PartitionedDiffReport partition(SnapshotDiffReport diffReport,
      SyncServiceFileFilter syncServiceFileFilter) {
    Map<ResultingOperation, List<DiffReportEntry>> triagedMap =
        diffReport
            .getDiffList()
            .stream()
            .filter(diffReportEntry -> diffReportEntry.getType() == RENAME)
            .collect(Collectors.groupingBy(diffReportEntry ->
                determineResultingOperation(diffReportEntry, syncServiceFileFilter)));

    List<DiffReportEntry> renames = triagedMap.getOrDefault(ResultingOperation.RENAME,
        Collections.emptyList());
    List<RenameEntryWithTemporaryName> renameEntries =
        getRenameEntriesAndGenerateTemporaryNames(
            renames);

    List<TranslatedEntry> translatedDeletes =
        handleDeletes(renameEntries,
            diffReport, syncServiceFileFilter);
    Collections.reverse(translatedDeletes);

    List<TranslatedEntry> translatedModifies =
        handleModifies(renameEntries,
            diffReport, syncServiceFileFilter);

    List<TranslatedEntry> translatedCreates =
        handleCreates(renameEntries,
            diffReport, syncServiceFileFilter);

    List<DiffReportEntry> createsFromRenames =
        triagedMap.getOrDefault(ResultingOperation.CREATE,
            Collections.emptyList());

    return new PartitionedDiffReport(renameEntries, translatedDeletes,
        translatedModifies, translatedCreates, createsFromRenames);
  }

  @VisibleForTesting
  static List<TranslatedEntry> handleDeletes(
      List<RenameEntryWithTemporaryName> renamedToTemps,
      SnapshotDiffReport diffReport,
      SyncServiceFileFilter syncServiceFileFilter) {

    return handleEntries(DELETE, PartitionedDiffReport::translateToTemporaryName,
        renamedToTemps, diffReport, syncServiceFileFilter);
  }

  @VisibleForTesting
  static List<TranslatedEntry> handleModifies(
      List<RenameEntryWithTemporaryName> renamedToTemps,
      SnapshotDiffReport diffReport,
      SyncServiceFileFilter syncServiceFileFilter) {

    return handleEntries(MODIFY, PartitionedDiffReport::translateToTargetName,
        renamedToTemps, diffReport, syncServiceFileFilter);
  }

  @VisibleForTesting
  static List<TranslatedEntry> handleCreates(
      List<RenameEntryWithTemporaryName> renamedToTemps,
      SnapshotDiffReport diffReport,
      SyncServiceFileFilter syncServiceFileFilter) {

    return handleEntries(CREATE, PartitionedDiffReport::translateToTargetName,
        renamedToTemps, diffReport, syncServiceFileFilter);
  }

  @VisibleForTesting
  static List<RenameEntryWithTemporaryName>
  getRenameEntriesAndGenerateTemporaryNames(List<DiffReportEntry> renameEntries) {
    return renameEntries
        .stream()
        .sorted(reverseSourceNameOrder)
        .map(RenameEntryWithTemporaryName::new)
        .collect(Collectors.toList());
  }

  static List<TranslatedEntry> handleEntries(DiffType diffType,
      BiFunction<DiffReportEntry, List<RenameEntryWithTemporaryName>,
          TranslatedEntry> translationFunction,
      List<RenameEntryWithTemporaryName> renamedToTemps,
      SnapshotDiffReport diffReport,
      SyncServiceFileFilter syncServiceFileFilter) {

    List<DiffReportEntry> entries = diffReport.getDiffList().stream()
        .filter(diffReportEntry -> diffReportEntry.getType() == diffType)
        .collect(Collectors.toList());

    List<TranslatedEntry> translatedEntries = entries
        .stream()
        .flatMap(entry -> {
          TranslatedEntry translatedEntry = translationFunction.apply(entry, renamedToTemps);
          if (syncServiceFileFilter.isExcluded(new File(translatedEntry.getTranslatedName()))) {
            return Stream.empty();
          } else {
            return Stream.of(translatedEntry);
          }
        })
        .collect(Collectors.toList());

    return translatedEntries;
  }

  private static TranslatedEntry translateToTemporaryName(DiffReportEntry entry,
      List<RenameEntryWithTemporaryName> renamesWithRenameEntryWithTemporaryNames) {

    for (RenameEntryWithTemporaryName renameItem :
        renamesWithRenameEntryWithTemporaryNames) {
      byte[] renameSourcePath = renameItem.getEntry().getSourcePath();
      byte[] sourcePath = entry.getSourcePath();
      if (sourcePath.equals(renameSourcePath)) {
        //if equal, this is two different things
      } else if (isParentOf(renameSourcePath,
          sourcePath)) {

        return TranslatedEntry.withTemporaryName(entry, renameItem);
      }
    }

    //No rename found. Keeping original name
    return TranslatedEntry.withNoRename(entry);

  }

  /**
   * Probe for a path being a parent of another.
   *
   * @param parent
   * @param child
   * @return true if the parent's path matches the start of the child's
   */
  private static boolean isParentOf(byte[] parent, byte[] child) {
    String parentPath = DFSUtil.bytes2String(parent);
    String childPath = DFSUtil.bytes2String(child);
    if (!parentPath.endsWith(Path.SEPARATOR)) {
      parentPath += Path.SEPARATOR;
    }

    return childPath.length() > parentPath.length() &&
        childPath.startsWith(parentPath);
  }

  private static TranslatedEntry translateToTargetName(DiffReportEntry entry,
      List<RenameEntryWithTemporaryName> renamesWithRenameEntryWithTemporaryNames) {

    for (RenameEntryWithTemporaryName renameItem :
        renamesWithRenameEntryWithTemporaryNames) {
      if (entry.getSourcePath().equals(renameItem.getEntry().getSourcePath())) {
        //if equal, this is two different things
      } else if (isParentOf(renameItem.getEntry().getSourcePath(),
          entry.getSourcePath())) {

        return TranslatedEntry.withTargetName(entry, renameItem);
      }
    }

    //No rename found. Keeping original name
    return TranslatedEntry.withNoRename(entry);

  }

  public List<RenameEntryWithTemporaryName> getRenames() {
    return renames;
  }

  public List<TranslatedEntry> getDeletes() {
    return deletes;
  }

  public List<TranslatedEntry> getModifies() {
    return modifies;
  }

  public List<TranslatedEntry> getCreates() {
    return creates;
  }

  public List<DiffReportEntry> getCreatesFromRenames() {
    return createsFromRenames;
  }

  public enum ResultingOperation {
    RENAME, CREATE, DELETE, NOOP
  }

  public static class RenameEntryWithTemporaryName {

    private DiffReportEntry entry;
    private String temporaryName;

    public RenameEntryWithTemporaryName(DiffReportEntry entry) {
      this.entry = entry;
      this.temporaryName = "tmp-" + UUID.randomUUID().toString();
    }

    public DiffReportEntry getEntry() {
      return entry;
    }

    public String getTemporaryName() {
      return temporaryName;
    }
  }

  public static class TranslatedEntry {
    private DiffReportEntry entry;
    private String translatedName;

    private TranslatedEntry(DiffReportEntry entry, String translatedName) {
      this.entry = entry;
      this.translatedName = translatedName;
    }

    public static TranslatedEntry withNoRename(DiffReportEntry entry) {
      return new TranslatedEntry(entry,
          DFSUtil.bytes2String(entry.getSourcePath()));
    }

    public static TranslatedEntry withTemporaryName(DiffReportEntry entry,
        RenameEntryWithTemporaryName renameItem) {
      String originalName = DFSUtil.bytes2String(entry.getSourcePath());

      String renameEntryName =
          DFSUtil.bytes2String(renameItem.getEntry().getSourcePath());


      //the next line can only work if this assert is true. Doublechecking...
      assert originalName.startsWith(renameEntryName);
      String translatedName = renameItem.getTemporaryName() +
          originalName.substring(renameEntryName.length());

      return new TranslatedEntry(entry, translatedName);
    }

    public static TranslatedEntry withTargetName(DiffReportEntry entry,
        RenameEntryWithTemporaryName renameItem) {
      String originalName = DFSUtil.bytes2String(entry.getSourcePath());

      String renameEntryName =
          DFSUtil.bytes2String(renameItem.getEntry().getSourcePath());


      //the next line can only work if this assert is true. Doublechecking...
      assert originalName.startsWith(renameEntryName);
      String translatedName = DFSUtil.bytes2String(renameItem.getEntry()
          .getTargetPath()) + originalName.substring(renameEntryName.length());

      return new TranslatedEntry(entry, translatedName);
    }

    public DiffReportEntry getEntry() {
      return entry;
    }

    public String getTranslatedName() {
      return translatedName;
    }
  }

}
