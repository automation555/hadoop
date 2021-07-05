/**
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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule.QueueMapping.MappingType;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;


public class UserGroupMappingPlacementRule extends PlacementRule {
  private static final Log LOG = LogFactory
      .getLog(UserGroupMappingPlacementRule.class);

  public static final String CURRENT_USER_MAPPING = "%user";

  public static final String PRIMARY_GROUP_MAPPING = "%primary_group";

  private boolean overrideWithQueueMappings = false;
  private List<QueueMapping> mappings = null;
  private Groups groups;

  @Private
  public static class QueueMapping {

    public enum MappingType {

      USER("u"), GROUP("g");
      private final String type;

      private MappingType(String type) {
        this.type = type;
      }

      public String toString() {
        return type;
      }

    };

    MappingType type;
    String source;
    String queue;

    public QueueMapping(MappingType type, String source, String queue) {
      this.type = type;
      this.source = source;
      this.queue = queue;
    }
    
    public String getQueue() {
      return queue;
    }
    
    @Override
    public int hashCode() {
      return super.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof QueueMapping) {
        QueueMapping other = (QueueMapping) obj;
        return (other.type.equals(type) && 
            other.source.equals(source) && 
            other.queue.equals(queue));
      } else {
        return false;
      }
    }
  }

  public UserGroupMappingPlacementRule(){
    this(false, null, null);
  }

  public UserGroupMappingPlacementRule(boolean overrideWithQueueMappings,
      List<QueueMapping> newMappings, Groups groups) {
    this.mappings = newMappings;
    this.overrideWithQueueMappings = overrideWithQueueMappings;
    this.groups = groups;
  }

  private String getMappedQueue(String user) throws IOException {
    for (QueueMapping mapping : mappings) {
      if (mapping.type == MappingType.USER) {
        if (mapping.source.equals(CURRENT_USER_MAPPING)) {
          if (mapping.queue.equals(CURRENT_USER_MAPPING)) {
            return user;
          } else if (mapping.queue.equals(PRIMARY_GROUP_MAPPING)) {
            return groups.getGroupsSet(user).iterator().next();
          } else {
            return mapping.queue;
          }
        }
        if (user.equals(mapping.source)) {
          return mapping.queue;
        }
      }
      if (mapping.type == MappingType.GROUP) {
        if (groups.getGroupsSet(user).contains(mapping.source)) {
          return mapping.queue;
        }
      }
    }
    return null;
  }

  @Override
  public String getQueueForApp(ApplicationSubmissionContext asc, String user)
      throws YarnException {
    String queueName = asc.getQueue();
    ApplicationId applicationId = asc.getApplicationId();
    if (mappings != null && mappings.size() > 0) {
      try {
        String mappedQueue = getMappedQueue(user);
        if (mappedQueue != null) {
          // We have a mapping, should we use it?
          if (queueName.equals(YarnConfiguration.DEFAULT_QUEUE_NAME)
              || overrideWithQueueMappings) {
            LOG.info("Application " + applicationId + " user " + user
                + " mapping [" + queueName + "] to [" + mappedQueue
                + "] override " + overrideWithQueueMappings);
            return mappedQueue;
          }
        }
      } catch (IOException ioex) {
        String message = "Failed to submit application " + applicationId +
            " submitted by user " + user + " reason: " + ioex.getMessage();
        throw new YarnException(message);
      }
    }
    
    return queueName;
  }

  @VisibleForTesting
  @Override
  public boolean initialize(ResourceScheduler scheduler)
      throws IOException {
    if (!(scheduler instanceof CapacityScheduler)) {
      throw new IOException(
          "UserGroupMappingPlacementRule can be configured only for "
              + "CapacityScheduler");
    }
    CapacitySchedulerContext schedulerContext =
        (CapacitySchedulerContext) scheduler;
    CapacitySchedulerConfiguration conf = schedulerContext.getConfiguration();
    boolean overrideWithQueueMappings = conf.getOverrideWithQueueMappings();
    LOG.info(
        "Initialized queue mappings, override: " + overrideWithQueueMappings);

    // Get new user/group mappings
    List<QueueMapping> newMappings = conf.getQueueMappings();

    CapacitySchedulerQueueManager queueManager =
        schedulerContext.getCapacitySchedulerQueueManager();

    // check if mappings refer to valid queues
    for (QueueMapping mapping : newMappings) {
      String mappingQueue = mapping.getQueue();
      if (!mappingQueue.equals(
          UserGroupMappingPlacementRule.CURRENT_USER_MAPPING) && !mappingQueue
          .equals(UserGroupMappingPlacementRule.PRIMARY_GROUP_MAPPING)) {
        CSQueue queue = queueManager.getQueue(mappingQueue);
        if (queue == null || !(queue instanceof LeafQueue)) {
          throw new IOException(
              "mapping contains invalid or non-leaf queue " + mappingQueue);
        }
      }
    }

    // initialize groups if mappings are present
    if (newMappings.size() > 0) {
      Groups groups = new Groups(conf);
      this.mappings = newMappings;
      this.groups = groups;
      this.overrideWithQueueMappings = overrideWithQueueMappings;
      return true;
    }

    return false;
  }

  @VisibleForTesting
  public List<QueueMapping> getQueueMappings() {
    return mappings;
  }
}
