/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.util;

import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotUtil.class);

  private SnapshotUtil() {
  }

  /**
   * Returns whether ancestorSnapshotId is an ancestor of snapshotId.
   */
  public static boolean ancestorOf(Table table, long snapshotId, long ancestorSnapshotId) {
    Snapshot current = table.snapshot(snapshotId);
    while (current != null) {
      long id = current.snapshotId();
      if (ancestorSnapshotId == id) {
        return true;
      } else if (current.parentId() != null) {
        current = table.snapshot(current.parentId());
      } else {
        return false;
      }
    }
    return false;
  }

  /**
   * Return the snapshot IDs for the ancestors of the current table state.
   * <p>
   * Ancestor IDs are ordered by commit time, descending. The first ID is the current snapshot, followed by its parent,
   * and so on.
   *
   * @param table a {@link Table}
   * @return a set of snapshot IDs of the known ancestor snapshots, including the current ID
   */
  public static List<Long> currentAncestors(Table table) {
    return ancestorIds(table.currentSnapshot(), table::snapshot);
  }

  /**
   * Returns list of snapshot ids in the range - (fromSnapshotId, toSnapshotId]
   * <p>
   * This method assumes that fromSnapshotId is an ancestor of toSnapshotId.
   */
  public static List<Long> snapshotIdsBetween(Table table, long fromSnapshotId, long toSnapshotId) {
    List<Long> snapshotIds = Lists.newArrayList(ancestorIds(table.snapshot(toSnapshotId),
        snapshotId -> snapshotId != fromSnapshotId ? table.snapshot(snapshotId) : null));
    return snapshotIds;
  }

  public static List<Long> ancestorIds(Snapshot snapshot, Function<Long, Snapshot> lookup) {
    List<Long> ancestorIds = Lists.newArrayList();
    Snapshot current = snapshot;
    while (current != null) {
      ancestorIds.add(current.snapshotId());
      if (current.parentId() != null) {
        current = lookup.apply(current.parentId());
      } else {
        current = null;
      }
    }
    return ancestorIds;
  }

  public static List<DataFile> newFiles(Long baseSnapshotId, long latestSnapshotId, Function<Long, Snapshot> lookup) {
    List<DataFile> newFiles = Lists.newArrayList();

    Long currentSnapshotId = latestSnapshotId;
    while (currentSnapshotId != null && !currentSnapshotId.equals(baseSnapshotId)) {
      Snapshot currentSnapshot = lookup.apply(currentSnapshotId);

      if (currentSnapshot == null) {
        throw new ValidationException(
            "Cannot determine history between read snapshot %s and current %s",
            baseSnapshotId, currentSnapshotId);
      }

      Iterables.addAll(newFiles, currentSnapshot.addedFiles());
      currentSnapshotId = currentSnapshot.parentId();
    }

    return newFiles;
  }

  /**
   * Given a timestamp, return the snapshot ID of the table that was current at this timestamp,
   * or null if such snapshot doesn't exist in table metadata.
   */
  public static Long snapshotIdFromTime(Table table, Long asOfTimestamp) {
    // history entries should already be ascending sorted by timestamp
    Long snapshotId = null;
    for (HistoryEntry historyEntry : table.history()) {
      if (asOfTimestamp >= historyEntry.timestampMillis()) {
        snapshotId = historyEntry.snapshotId();
      }
    }
    return snapshotId;
  }

  /**
   * Returns the schema associated with the snapshot derived from the given snapshot ID.
   * <p>
   * The method throws exception if the input snapshot ID is null.
   * Returns null if no snapshot is found with the given ID, or no schema ID is associated with this
   * snapshot, or no schema is associated with the schema ID from table's metadata.
   * <p>
   * Caller of this method is responsible for passing in a snapshot ID that exists in history entry,
   * to ensure that the snapshot is indeed an actual previous state of the table.
   */
  public static Schema schemaOfSnapshot(Table table, Long snapshotId) {
    Preconditions.checkNotNull(snapshotId, "SnapshotId is required for looking up schema. ");

    if (table.snapshot(snapshotId) != null && table.snapshot(snapshotId).schemaId() != null) {
      Schema schema = table.schemas().get(table.snapshot(snapshotId).schemaId());
      if (schema != null) {
        return schema;
      }
    }

    LOG.error("Cannot find schema from snapshot with snapshot ID {}.", snapshotId);
    return null;
  }
}
