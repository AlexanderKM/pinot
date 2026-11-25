/**
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
package org.apache.pinot.broker.routing.segmentpruner;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionInfo;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.sql.FilterKind;


/// An inverted index from partition to segments, used by the partition pruners to find the segments matching a filter
/// without looking at every candidate segment.
///
/// For each partition column the index holds, per partition function, the set of segments in each partition
/// (`partition function -> partition id -> segments`), plus the segments that carry no usable partition metadata for
/// that column. The latter match every value of the column, because a segment whose partitions are unknown can never be
/// ruled out.
///
/// Only partition functions that opt into [PartitionFunction#supportsPartitionIdPreparation()] are indexed, because
/// that is what guarantees that equal functions behave identically and can share a group. A segment using any other
/// function is recorded in [#hasUnindexableSegments()] and the caller must not use the index while there is one.
///
/// ### Thread-safety
/// An instance is immutable once built and may be read by any number of threads. Writes derive a new instance from the
/// previous one with [#applyChanges(List)], leaving the previous one intact, so a reader holding the old instance sees
/// a consistent view. Callers publish the new instance through a `volatile` field and serialize their writes.
///
/// ### Cost of a write
/// A change copies only the buckets it touches: the segment sets of the (function, partition) pairs the segment enters
/// or leaves, and the small maps leading to them. Every other bucket is shared by reference with the previous
/// instance. A segment refreshed without any change to its partitions copies nothing and returns the same instance.
final class PartitionIndex {
  private final Map<String, ColumnIndex> _columns;
  private final Set<String> _unindexableSegments;

  private PartitionIndex(Map<String, ColumnIndex> columns, Set<String> unindexableSegments) {
    _columns = columns;
    _unindexableSegments = unindexableSegments;
  }

  /// Returns an index with no segments for the given partition columns.
  static PartitionIndex empty(Set<String> partitionColumns) {
    Map<String, ColumnIndex> columns = new HashMap<>();
    for (String partitionColumn : partitionColumns) {
      columns.put(partitionColumn, ColumnIndex.EMPTY);
    }
    return new PartitionIndex(columns, Set.of());
  }

  /// Returns `true` if some segment uses a partition function that cannot be indexed, in which case the index does not
  /// describe that segment and must not be used to prune.
  boolean hasUnindexableSegments() {
    return !_unindexableSegments.isEmpty();
  }

  /// Returns the segments of `scope` that may satisfy the filter according to their partition metadata, or `null` if
  /// the filter does not constrain any partition column (so every segment of `scope` may match).
  ///
  /// This is the result of evaluating the filter on each segment separately, computed on partitions instead of on
  /// segments. A leaf on a partition column matches the segments whose partitions include one of its values, plus the
  /// segments without partition metadata for that column. An `AND` ignores the children that do not constrain a
  /// partition column. An `OR` does not constrain anything if one of its children does not, because a segment could
  /// satisfy that child. Anything else does not constrain either.
  ///
  /// The returned set is a new set owned by the caller.
  @Nullable
  Set<String> resolve(Expression filterExpression, Set<String> scope) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();
    switch (filterKind) {
      case AND: {
        Set<String> result = null;
        for (Expression child : operands) {
          Set<String> childResult = resolve(child, scope);
          if (childResult != null) {
            result = result != null ? intersect(result, childResult) : childResult;
          }
        }
        return result;
      }
      case OR: {
        Set<String> result = new HashSet<>();
        for (Expression child : operands) {
          Set<String> childResult = resolve(child, scope);
          if (childResult == null) {
            return null;
          }
          result.addAll(childResult);
        }
        return result;
      }
      case EQUALS:
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier == null) {
          return null;
        }
        ColumnIndex columnIndex = _columns.get(identifier.getName());
        if (columnIndex == null) {
          return null;
        }
        int numValues = filterKind == FilterKind.EQUALS ? 1 : operands.size() - 1;
        List<String> values = new ArrayList<>(numValues);
        for (int i = 1; i <= numValues; i++) {
          values.add(RequestContextUtils.getStringValue(operands.get(i)));
        }
        return columnIndex.match(values, scope);
      }
      default:
        return null;
    }
  }

  /// Returns the index that results from applying the changes in order, or this instance if none of them changes
  /// anything the index records.
  PartitionIndex applyChanges(List<SegmentChange> changes) {
    Update update = new Update(this);
    for (SegmentChange change : changes) {
      update.apply(change);
    }
    return update.build();
  }

  private static Set<String> intersect(Set<String> first, Set<String> second) {
    Set<String> smaller = first.size() <= second.size() ? first : second;
    Set<String> larger = smaller == first ? second : first;
    Set<String> result = new HashSet<>();
    for (String segment : smaller) {
      if (larger.contains(segment)) {
        result.add(segment);
      }
    }
    return result;
  }

  /// A segment's partition metadata moving from `oldInfos` to `newInfos`, each a map from partition column to the
  /// segment's partition info for that column. A column the segment has no usable info for is left out of the map, and
  /// the segment is then treated as matching every value of that column. `null` means that the segment is not in the
  /// index, so `oldInfos == null` adds a segment and `newInfos == null` removes one.
  record SegmentChange(String segment, @Nullable Map<String, SegmentPartitionInfo> oldInfos,
                       @Nullable Map<String, SegmentPartitionInfo> newInfos) {
  }

  /// The index of one partition column.
  private static final class ColumnIndex {
    static final ColumnIndex EMPTY = new ColumnIndex(Map.of(), Set.of());

    /// `partition function -> partition id -> segments in that partition`. Never contains an empty group or bucket.
    final Map<PartitionFunction, Map<Integer, Set<String>>> _buckets;
    /// Segments without usable partition info for this column. They match every value.
    final Set<String> _unconstrainedSegments;

    ColumnIndex(Map<PartitionFunction, Map<Integer, Set<String>>> buckets, Set<String> unconstrainedSegments) {
      _buckets = buckets;
      _unconstrainedSegments = unconstrainedSegments;
    }

    /// Returns the segments of `scope` in a partition of any of the values, or without partition info for the column.
    Set<String> match(List<String> values, Set<String> scope) {
      List<Set<String>> candidates = new ArrayList<>();
      for (Map.Entry<PartitionFunction, Map<Integer, Set<String>>> entry : _buckets.entrySet()) {
        PartitionFunction function = entry.getKey();
        Map<Integer, Set<String>> partitionToSegments = entry.getValue();
        IntSet partitions = new IntOpenHashSet(values.size());
        for (String value : values) {
          partitions.add(function.getPartition(value));
        }
        IntIterator iterator = partitions.iterator();
        while (iterator.hasNext()) {
          Set<String> segments = partitionToSegments.get(iterator.nextInt());
          if (segments != null) {
            candidates.add(segments);
          }
        }
      }
      if (!_unconstrainedSegments.isEmpty()) {
        candidates.add(_unconstrainedSegments);
      }
      return collect(candidates, scope);
    }

    /// Returns the union of `candidates` restricted to `scope`, walking whichever side is cheaper: every segment of the
    /// scope against the candidates, or every candidate against the scope.
    private static Set<String> collect(List<Set<String>> candidates, Set<String> scope) {
      Set<String> result = new HashSet<>();
      if (candidates.isEmpty()) {
        return result;
      }
      long numCandidateSegments = 0;
      for (Set<String> segments : candidates) {
        numCandidateSegments += segments.size();
      }
      if ((long) scope.size() * candidates.size() <= numCandidateSegments) {
        for (String segment : scope) {
          for (Set<String> segments : candidates) {
            if (segments.contains(segment)) {
              result.add(segment);
              break;
            }
          }
        }
      } else {
        for (Set<String> segments : candidates) {
          for (String segment : segments) {
            if (scope.contains(segment)) {
              result.add(segment);
            }
          }
        }
      }
      return result;
    }
  }

  /// Applies a batch of changes to a base index, copying each piece of the base the first time it is written to.
  private static final class Update {
    private final PartitionIndex _base;
    private final Map<String, ColumnUpdate> _columnUpdates = new HashMap<>();
    private Set<String> _unindexableSegments;
    private boolean _unindexableSegmentsCopied;
    private boolean _changed;

    Update(PartitionIndex base) {
      _base = base;
      _unindexableSegments = base._unindexableSegments;
    }

    void apply(SegmentChange change) {
      String segment = change.segment();
      Map<String, SegmentPartitionInfo> oldInfos = change.oldInfos();
      Map<String, SegmentPartitionInfo> newInfos = change.newInfos();
      for (String column : _base._columns.keySet()) {
        applyColumn(column, segment, oldInfos != null, oldInfos != null ? oldInfos.get(column) : null,
            newInfos != null, newInfos != null ? newInfos.get(column) : null);
      }
      boolean wasUnindexable = hasUnindexableInfo(oldInfos);
      boolean isUnindexable = hasUnindexableInfo(newInfos);
      if (wasUnindexable != isUnindexable) {
        if (!_unindexableSegmentsCopied) {
          _unindexableSegments = new HashSet<>(_unindexableSegments);
          _unindexableSegmentsCopied = true;
        }
        if (isUnindexable) {
          _unindexableSegments.add(segment);
        } else {
          _unindexableSegments.remove(segment);
        }
        _changed = true;
      }
    }

    private void applyColumn(String column, String segment, boolean wasIndexed, @Nullable SegmentPartitionInfo oldInfo,
        boolean isIndexed, @Nullable SegmentPartitionInfo newInfo) {
      if (!wasIndexed && !isIndexed) {
        return;
      }
      boolean wasBucketed = wasIndexed && isIndexable(oldInfo);
      boolean isBucketed = isIndexed && isIndexable(newInfo);
      boolean wasUnconstrained = wasIndexed && !wasBucketed;
      boolean isUnconstrained = isIndexed && !isBucketed;
      if (wasUnconstrained && isUnconstrained) {
        return;
      }
      if (wasBucketed && isBucketed && oldInfo.getPartitionFunction().equals(newInfo.getPartitionFunction())
          && oldInfo.getPartitions().equals(newInfo.getPartitions())) {
        return;
      }

      ColumnUpdate columnUpdate = _columnUpdates.computeIfAbsent(column, c -> new ColumnUpdate(_base._columns.get(c)));
      if (wasUnconstrained) {
        columnUpdate.removeUnconstrained(segment);
      }
      if (isUnconstrained) {
        columnUpdate.addUnconstrained(segment);
      }
      if (wasBucketed && isBucketed && oldInfo.getPartitionFunction().equals(newInfo.getPartitionFunction())) {
        // Same function: only the partitions that differ move
        PartitionFunction function = newInfo.getPartitionFunction();
        for (int partition : oldInfo.getPartitions()) {
          if (!newInfo.getPartitions().contains(partition)) {
            columnUpdate.remove(function, partition, segment);
          }
        }
        for (int partition : newInfo.getPartitions()) {
          if (!oldInfo.getPartitions().contains(partition)) {
            columnUpdate.add(function, partition, segment);
          }
        }
      } else {
        if (wasBucketed) {
          for (int partition : oldInfo.getPartitions()) {
            columnUpdate.remove(oldInfo.getPartitionFunction(), partition, segment);
          }
        }
        if (isBucketed) {
          for (int partition : newInfo.getPartitions()) {
            columnUpdate.add(newInfo.getPartitionFunction(), partition, segment);
          }
        }
      }
      _changed = true;
    }

    private static boolean isIndexable(@Nullable SegmentPartitionInfo info) {
      return info != null && info.getPartitionFunction().supportsPartitionIdPreparation();
    }

    private static boolean hasUnindexableInfo(@Nullable Map<String, SegmentPartitionInfo> infos) {
      if (infos == null) {
        return false;
      }
      for (SegmentPartitionInfo info : infos.values()) {
        if (info != null && !isIndexable(info)) {
          return true;
        }
      }
      return false;
    }

    PartitionIndex build() {
      if (!_changed) {
        return _base;
      }
      Map<String, ColumnIndex> columns = new HashMap<>(_base._columns);
      for (Map.Entry<String, ColumnUpdate> entry : _columnUpdates.entrySet()) {
        columns.put(entry.getKey(), entry.getValue().build());
      }
      return new PartitionIndex(columns, _unindexableSegments);
    }
  }

  /// The working copy of one column's index: starts out sharing everything with the base, and copies a map or a
  /// segment set the first time it is changed. Copies are never visible to readers before [#build()].
  private static final class ColumnUpdate {
    private final ColumnIndex _base;
    private Map<PartitionFunction, Map<Integer, Set<String>>> _buckets;
    private boolean _bucketsCopied;
    private final Set<PartitionFunction> _copiedFunctions = new HashSet<>();
    private final Set<Bucket> _copiedBuckets = new HashSet<>();
    private Set<String> _unconstrainedSegments;
    private boolean _unconstrainedSegmentsCopied;

    ColumnUpdate(ColumnIndex base) {
      _base = base;
      _buckets = base._buckets;
      _unconstrainedSegments = base._unconstrainedSegments;
    }

    void add(PartitionFunction function, int partition, String segment) {
      Map<Integer, Set<String>> partitionToSegments = mutablePartitions(function, true);
      Bucket bucket = new Bucket(function, partition);
      Set<String> segments = partitionToSegments.get(partition);
      if (segments == null) {
        segments = new HashSet<>();
        partitionToSegments.put(partition, segments);
        _copiedBuckets.add(bucket);
      } else if (_copiedBuckets.add(bucket)) {
        segments = new HashSet<>(segments);
        partitionToSegments.put(partition, segments);
      }
      segments.add(segment);
    }

    void remove(PartitionFunction function, int partition, String segment) {
      Map<Integer, Set<String>> partitionToSegments = mutablePartitions(function, false);
      if (partitionToSegments == null) {
        return;
      }
      Set<String> segments = partitionToSegments.get(partition);
      if (segments == null) {
        return;
      }
      Bucket bucket = new Bucket(function, partition);
      if (_copiedBuckets.add(bucket)) {
        segments = new HashSet<>(segments);
        partitionToSegments.put(partition, segments);
      }
      segments.remove(segment);
      if (segments.isEmpty()) {
        partitionToSegments.remove(partition);
        _copiedBuckets.remove(bucket);
        if (partitionToSegments.isEmpty()) {
          _buckets.remove(function);
          _copiedFunctions.remove(function);
        }
      }
    }

    void addUnconstrained(String segment) {
      copyUnconstrainedSegments();
      _unconstrainedSegments.add(segment);
    }

    void removeUnconstrained(String segment) {
      copyUnconstrainedSegments();
      _unconstrainedSegments.remove(segment);
    }

    ColumnIndex build() {
      return new ColumnIndex(_buckets, _unconstrainedSegments);
    }

    /// Returns the working copy of the function's partition map, or `null` if there is none and `create` is `false`.
    @Nullable
    private Map<Integer, Set<String>> mutablePartitions(PartitionFunction function, boolean create) {
      if (!_bucketsCopied) {
        _buckets = new HashMap<>(_base._buckets);
        _bucketsCopied = true;
      }
      Map<Integer, Set<String>> partitionToSegments = _buckets.get(function);
      if (partitionToSegments == null) {
        if (!create) {
          return null;
        }
        partitionToSegments = new HashMap<>();
        _buckets.put(function, partitionToSegments);
        _copiedFunctions.add(function);
      } else if (_copiedFunctions.add(function)) {
        partitionToSegments = new HashMap<>(partitionToSegments);
        _buckets.put(function, partitionToSegments);
      }
      return partitionToSegments;
    }

    private void copyUnconstrainedSegments() {
      if (!_unconstrainedSegmentsCopied) {
        _unconstrainedSegments = new HashSet<>(_unconstrainedSegments);
        _unconstrainedSegmentsCopied = true;
      }
    }
  }

  private record Bucket(PartitionFunction function, int partition) {
  }
}
