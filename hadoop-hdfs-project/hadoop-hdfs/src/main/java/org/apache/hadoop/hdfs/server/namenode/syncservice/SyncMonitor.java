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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedSyncMountSnapshotUpdateFactory;
import org.apache.hadoop.hdfs.server.namenode.syncservice.scheduler.SyncTaskScheduler;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SchedulableSyncPhase;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SyncMountSnapshotUpdateTracker;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SyncMountSnapshotUpdateTrackerFactory;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class SyncMonitor {

  public static final Logger LOG = LoggerFactory.getLogger(SyncMonitor.class);
  private final Namesystem namesystem;
  private Map<String, SyncMountSnapshotUpdateTracker> inProgress;
  private Map<String, SyncMountSnapshotUpdateTracker> trackersFailed;
  private PhasedSyncMountSnapshotUpdateFactory syncMountSnapshotUpdatePlanFactory;
  private SyncTaskScheduler syncTaskScheduler;
  private BlockAliasMap.Writer<FileRegion> aliasMapWriter;
  private Configuration conf;

  public SyncMonitor(Namesystem namesystem, PhasedSyncMountSnapshotUpdateFactory
      syncMountSnapshotUpdatePlanFactory, SyncTaskScheduler syncTaskScheduler,
      BlockAliasMap.Writer<FileRegion> aliasMapWriter, Configuration conf) {
    this.syncMountSnapshotUpdatePlanFactory = syncMountSnapshotUpdatePlanFactory;
    this.syncTaskScheduler = syncTaskScheduler;
    this.aliasMapWriter = aliasMapWriter;
    this.conf = conf;
    this.inProgress = Maps.newConcurrentMap();
    this.trackersFailed = Maps.newConcurrentMap();
    this.namesystem = namesystem;
  }

  public boolean markSyncTaskFailed(UUID syncTaskId,
      String syncMountId,
      SyncTaskExecutionResult result) {

    Optional<SyncMountSnapshotUpdateTracker> syncMountSnapshotUpdateTrackerOpt =
        fetchUpdateTracker(syncMountId);

    return syncMountSnapshotUpdateTrackerOpt.map(syncMountSnapshotUpdateTracker -> {
      boolean isTrackerStillValid = syncMountSnapshotUpdateTracker.markFailed(syncTaskId, result);
      if (isTrackerStillValid) {
        return true;
      } else {
        inProgress.remove(syncMountId);
        this.trackersFailed.put(syncMountId, syncMountSnapshotUpdateTracker);
        LOG.error("Issue when remounting sync mount after failed tracker. Tracker removed from trackers in progress.");
        return false;
      }
    })
        .orElse(false);
  }

  public void markSyncTaskFinished(UUID syncTaskId, String syncMountId,
      SyncTaskExecutionResult result) {

    Optional<SyncMountSnapshotUpdateTracker> syncMountSnapshotUpdateTrackerOpt = fetchUpdateTracker(syncMountId);

    syncMountSnapshotUpdateTrackerOpt.ifPresent(syncMountSnapshotUpdateTracker -> {
      syncMountSnapshotUpdateTracker.markFinished(syncTaskId, result);

      if (syncMountSnapshotUpdateTracker.isFinished()) {
        inProgress.remove(syncMountId);

        //No notification, as this is a timeout thing in the SyncServiceSatisfier
      } else {
        this.notify();
      }
    });

  }

  @VisibleForTesting
  boolean hasTrackersInProgress() {
    return !inProgress.isEmpty();
  }

  private Optional<SyncMountSnapshotUpdateTracker> fetchUpdateTracker(String syncMountId) {
    SyncMountSnapshotUpdateTracker syncMountSnapshotUpdateTracker =
        inProgress.get(syncMountId);

    if (syncMountSnapshotUpdateTracker == null) {
      // This can happen when the tracker failed, was removed, and the
      // full resync was not scheduled
      // @ehiggs not sure whether this is really necessary, rediscuss at review time.
      return Optional.empty();
    }
    return Optional.of(syncMountSnapshotUpdateTracker);
  }

  void scheduleNextWork() {

    MountManager mountManager = namesystem.getMountManager();
    List<SyncMount> syncMounts = mountManager.getSyncMounts();

    for (SyncMount syncMount : syncMounts) {
      if (namesystem.isInSafeMode()) {
        LOG.debug("Skipping synchronization of SyncMounts as the " +
            "namesystem is in safe mode");
        break;
      }
      if (syncMount.isPaused()) {
        LOG.info("Sync mount is paused");
        continue;
      }

      if (inProgress.containsKey(syncMount.getName())) {
        scheduleNextWorkOnTracker(inProgress.get(syncMount.getName()), syncMount);
      } else {
        scheduleNewSyncMountSnapshotUpdate(syncMount);
      }

    }
  }

  public void fullResync(String syncMountId, BlockAliasMap.Reader<FileRegion> aliasMapReader) throws IOException {
    MountManager mountManager = namesystem.getMountManager();
    SyncMount syncMount = mountManager.getSyncMount(syncMountId);
    SnapshotDiffReport diffReport =
        mountManager.forceInitialSnapshot(syncMount.getLocalPath());
    int targetSnapshotId = getTargetSnapshotId(diffReport);
    Optional<Integer> sourceSnapshotId = Optional.empty();
    PhasedPlan planFromDiffReport = syncMountSnapshotUpdatePlanFactory.
        createPlanFromDiffReport(syncMount, diffReport, sourceSnapshotId,
            targetSnapshotId);
    for (FileRegion fileRegion : aliasMapReader) {
      //TODO add nonce
      Path pathInAliasMap = fileRegion.getProvidedStorageLocation().getPath();
      planFromDiffReport.filter(pathInAliasMap);
    }
    SyncMountSnapshotUpdateTracker tracker =
        SyncMountSnapshotUpdateTrackerFactory.create(planFromDiffReport,
            aliasMapWriter, conf);
    scheduleNextWorkOnTracker(tracker, syncMount);
  }

  public boolean blockingCancelTracker(String syncMountId) {
    //Do not remove. Let the mark fail/mark success do this through the normal process
    SyncMountSnapshotUpdateTracker syncMountSnapshotUpdateTracker = inProgress.get(syncMountId);
    if (syncMountSnapshotUpdateTracker == null) {
      //Possible that the tracker already finished by the time the request to
      //cancel comes in.
      return true;
    }
    return syncMountSnapshotUpdateTracker.blockingCancel();
  }

  private void scheduleNewSyncMountSnapshotUpdate(SyncMount syncMount) {

    LOG.info("Planning new SyncMount {}", syncMount);

    if (inProgress.containsKey(syncMount.getName())) {
      LOG.info("SyncMount {} still has unfinished scheduled work, not adding " +
          "additional work", syncMount);
    } else {

      MountManager mountManager = namesystem.getMountManager();

      SnapshotDiffReport diffReport;
      Optional<Integer> sourceSnapshotId;
      int targetSnapshotId;
      try {
        diffReport = mountManager.makeSnapshotAndPerformDiff(
            syncMount.getLocalPath());
        sourceSnapshotId = getSourceSnapshotId(diffReport);
        targetSnapshotId = getTargetSnapshotId(diffReport);
      } catch (IOException e) {
        LOG.error("Failed to take snapshot for: {}", syncMount, e);
        return;
      }

      PhasedPlan planFromDiffReport = syncMountSnapshotUpdatePlanFactory.
          createPlanFromDiffReport(syncMount, diffReport, sourceSnapshotId,
              targetSnapshotId);

      if (planFromDiffReport.isEmpty()) {
        /**
         * The tracker for an empty plan will never finish as there will
         * be no tasks to trigger the finish marking.
         */
        LOG.info("Empty plan, not starting a tracker");
      } else {
        SyncMountSnapshotUpdateTracker tracker =
            SyncMountSnapshotUpdateTrackerFactory.create(planFromDiffReport,
                aliasMapWriter, conf);
        scheduleNextWorkOnTracker(tracker, syncMount);
      }
    }
  }

  private void scheduleNextWorkOnTracker(SyncMountSnapshotUpdateTracker tracker, SyncMount syncMount) {
    inProgress.put(syncMount.getName(), tracker);
    SchedulableSyncPhase schedulableSyncPhase = tracker.getNextSchedulablePhase();
    syncTaskScheduler.schedule(schedulableSyncPhase);
  }

  private Optional<Integer> getSourceSnapshotId(SnapshotDiffReport diffReport)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    if (diffReport.getFromSnapshot() == null) {
      return Optional.empty();
    }
    INode localBackupPathINode = namesystem.getFSDirectory()
        .getINode(diffReport.getSnapshotRoot());
    INodeDirectory localBackupPathINodeDirectory =
        localBackupPathINode.asDirectory();
    Snapshot toSnapshot = localBackupPathINodeDirectory.getSnapshot(
        diffReport.getFromSnapshot().getBytes());
    return Optional.of(toSnapshot.getId());
  }

  private int getTargetSnapshotId(SnapshotDiffReport diffReport)
      throws UnresolvedLinkException, AccessControlException,
      ParentNotDirectoryException {
    INode localBackupPathINode = namesystem.getFSDirectory()
        .getINode(diffReport.getSnapshotRoot());
    INodeDirectory localBackupPathINodeDirectory =
        localBackupPathINode.asDirectory();
    Snapshot toSnapshot = localBackupPathINodeDirectory.getSnapshot(
        diffReport.getLaterSnapshotName().getBytes());
    return toSnapshot.getId();
  }


  public Set<String> getTrackersInProgress() {
    return this.inProgress.keySet();
  }

  public Set<String> getTrackersFailed() {
    return this.trackersFailed.keySet();
  }

}
