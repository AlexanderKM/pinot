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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionInfo;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionUtils;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.sql.FilterKind;


/// The `MultiPartitionColumnsSegmentPruner` prunes segments based on their partition metadata stored in ZK. The
/// pruner supports queries with filter (or nested filter) of EQUALITY and IN predicates.
///
/// There are two ways to prune, with the same result when their preconditions hold:
///   - [#prune]: looks at every candidate segment, so its cost grows with the number of candidates. A candidate the
///     pruner knows nothing about is always kept. This works for any candidates.
///   - [#pruneAssumingSyncedScope]: looks the filter up in a [PartitionIndex] (partition -> segments), so its cost
///     grows with the number of segments that match, not with the number of candidates. It relies on the candidates
///     having been selected from the same segment assignment the pruner has been updated with, because it does not look
///     at the candidates that are not in the index. The caller is responsible for checking that, see
///     [SegmentPruner#pruneAssumingSyncedScope].
///
/// Thread-safety: updates are synchronized and queries are not. `_segmentColumnPartitionInfoMap` is concurrent, and
/// `_index` is an immutable snapshot that is replaced by a single volatile write, so a query sees either the index from
/// before an update or the one from after it.
public class MultiPartitionColumnsSegmentPruner implements SegmentPruner {
  private final String _tableNameWithType;
  private final Set<String> _partitionColumns;
  private final Map<String, Map<String, SegmentPartitionInfo>> _segmentColumnPartitionInfoMap =
      new ConcurrentHashMap<>();
  // Online segments whose partition info could not be extracted yet (their ZK metadata is missing). They are not in
  // _segmentColumnPartitionInfoMap, so that extraction is retried, but they are in the index, where they match every
  // value. Only touched by init() and the synchronized updates.
  private final Set<String> _unresolvedSegments = new HashSet<>();
  private volatile PartitionIndex _index;

  public MultiPartitionColumnsSegmentPruner(String tableNameWithType, Set<String> partitionColumns) {
    _tableNameWithType = tableNameWithType;
    _partitionColumns = partitionColumns;
    _index = PartitionIndex.empty(partitionColumns);
  }

  @Override
  public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
      List<ZNRecord> znRecords) {
    // Bulk load partition info for all online segments
    List<PartitionIndex.SegmentChange> changes = new ArrayList<>(onlineSegments.size());
    for (int idx = 0; idx < onlineSegments.size(); idx++) {
      String segment = onlineSegments.get(idx);
      Map<String, SegmentPartitionInfo> columnPartitionInfoMap =
          SegmentPartitionUtils.extractPartitionInfoMap(_tableNameWithType, _partitionColumns, segment,
              znRecords.get(idx));
      if (columnPartitionInfoMap != null) {
        _segmentColumnPartitionInfoMap.put(segment, columnPartitionInfoMap);
        changes.add(new PartitionIndex.SegmentChange(segment, null, columnPartitionInfoMap));
      } else {
        _unresolvedSegments.add(segment);
        changes.add(new PartitionIndex.SegmentChange(segment, null, Map.of()));
      }
    }
    _index = _index.applyChanges(changes);
  }

  @Override
  public synchronized void onAssignmentChange(IdealState idealState, ExternalView externalView,
      Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
    // NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
    //       ones. The refreshed segment ZK metadata change won't be picked up.
    List<PartitionIndex.SegmentChange> changes = new ArrayList<>();
    for (int idx = 0; idx < pulledSegments.size(); idx++) {
      String segment = pulledSegments.get(idx);
      if (_segmentColumnPartitionInfoMap.containsKey(segment)) {
        continue;
      }
      Map<String, SegmentPartitionInfo> columnPartitionInfoMap =
          SegmentPartitionUtils.extractPartitionInfoMap(_tableNameWithType, _partitionColumns, segment,
              znRecords.get(idx));
      boolean wasUnresolved = _unresolvedSegments.contains(segment);
      if (columnPartitionInfoMap != null) {
        _segmentColumnPartitionInfoMap.put(segment, columnPartitionInfoMap);
        _unresolvedSegments.remove(segment);
        changes.add(new PartitionIndex.SegmentChange(segment, wasUnresolved ? Map.of() : null,
            columnPartitionInfoMap));
      } else if (!wasUnresolved) {
        _unresolvedSegments.add(segment);
        changes.add(new PartitionIndex.SegmentChange(segment, null, Map.of()));
      }
    }
    Iterator<Map.Entry<String, Map<String, SegmentPartitionInfo>>> infoIterator =
        _segmentColumnPartitionInfoMap.entrySet().iterator();
    while (infoIterator.hasNext()) {
      Map.Entry<String, Map<String, SegmentPartitionInfo>> entry = infoIterator.next();
      if (!onlineSegments.contains(entry.getKey())) {
        changes.add(new PartitionIndex.SegmentChange(entry.getKey(), entry.getValue(), null));
        infoIterator.remove();
      }
    }
    Iterator<String> unresolvedIterator = _unresolvedSegments.iterator();
    while (unresolvedIterator.hasNext()) {
      String segment = unresolvedIterator.next();
      if (!onlineSegments.contains(segment)) {
        changes.add(new PartitionIndex.SegmentChange(segment, Map.of(), null));
        unresolvedIterator.remove();
      }
    }
    _index = _index.applyChanges(changes);
  }

  @Override
  public synchronized void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
    Map<String, SegmentPartitionInfo> oldColumnPartitionInfo = _segmentColumnPartitionInfoMap.get(segment);
    Map<String, SegmentPartitionInfo> oldIndexedInfos = oldColumnPartitionInfo;
    if (oldIndexedInfos == null && _unresolvedSegments.contains(segment)) {
      oldIndexedInfos = Map.of();
    }
    Map<String, SegmentPartitionInfo> columnPartitionInfo =
        SegmentPartitionUtils.extractPartitionInfoMap(_tableNameWithType, _partitionColumns, segment, znRecord);
    Map<String, SegmentPartitionInfo> newIndexedInfos;
    if (columnPartitionInfo != null) {
      _segmentColumnPartitionInfoMap.put(segment, columnPartitionInfo);
      _unresolvedSegments.remove(segment);
      newIndexedInfos = columnPartitionInfo;
    } else {
      _segmentColumnPartitionInfoMap.remove(segment);
      _unresolvedSegments.add(segment);
      newIndexedInfos = Map.of();
    }
    _index = _index.applyChanges(List.of(new PartitionIndex.SegmentChange(segment, oldIndexedInfos, newIndexedInfos)));
  }

  @Override
  public Set<String> pruneAssumingSyncedScope(BrokerRequest brokerRequest, Set<String> segments) {
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    if (filterExpression == null) {
      return segments;
    }
    PartitionIndex index = _index;
    if (index.hasUnindexableSegments()) {
      return prune(brokerRequest, segments);
    }
    try {
      Set<String> matchingSegments = index.resolve(filterExpression, segments);
      return matchingSegments != null ? matchingSegments : segments;
    } catch (RuntimeException e) {
      // The index looks up every value of the filter, which can reach a value (or a branch) that evaluating segments
      // one by one would never reach. Let that path decide, since it is the one that defines the behavior.
      return prune(brokerRequest, segments);
    }
  }

  @Override
  public Set<String> prune(BrokerRequest brokerRequest, Set<String> segments) {
    Expression filterExpression = brokerRequest.getPinotQuery().getFilterExpression();
    if (filterExpression == null) {
      return segments;
    }
    Set<String> selectedSegments = new HashSet<>();
    for (String segment : segments) {
      Map<String, SegmentPartitionInfo> columnPartitionInfoMap = _segmentColumnPartitionInfoMap.get(segment);
      if (columnPartitionInfoMap == null
          || columnPartitionInfoMap == SegmentPartitionUtils.INVALID_COLUMN_PARTITION_INFO_MAP || isPartitionMatch(
          filterExpression, columnPartitionInfoMap)) {
        selectedSegments.add(segment);
      }
    }
    return selectedSegments;
  }

  @VisibleForTesting
  public Set<String> getPartitionColumns() {
    return _partitionColumns;
  }

  private boolean isPartitionMatch(Expression filterExpression,
      Map<String, SegmentPartitionInfo> columnPartitionInfoMap) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());
    List<Expression> operands = function.getOperands();
    switch (filterKind) {
      case AND:
        for (Expression child : operands) {
          if (!isPartitionMatch(child, columnPartitionInfoMap)) {
            return false;
          }
        }
        return true;
      case OR:
        for (Expression child : operands) {
          if (isPartitionMatch(child, columnPartitionInfoMap)) {
            return true;
          }
        }
        return false;
      case EQUALS: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null) {
          SegmentPartitionInfo partitionInfo = columnPartitionInfoMap.get(identifier.getName());
          return partitionInfo == null || partitionInfo.getPartitions().contains(
              partitionInfo.getPartitionFunction().getPartition(RequestContextUtils.getStringValue(operands.get(1))));
        } else {
          return true;
        }
      }
      case IN: {
        Identifier identifier = operands.get(0).getIdentifier();
        if (identifier != null) {
          SegmentPartitionInfo partitionInfo = columnPartitionInfoMap.get(identifier.getName());
          if (partitionInfo == null) {
            return true;
          }
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (partitionInfo.getPartitions().contains(partitionInfo.getPartitionFunction()
                .getPartition(RequestContextUtils.getStringValue(operands.get(i))))) {
              return true;
            }
          }
          return false;
        } else {
          return true;
        }
      }
      default:
        return true;
    }
  }
}
