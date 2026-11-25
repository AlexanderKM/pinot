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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionInfo;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Checks that pruning through the [PartitionIndex] gives exactly the result of the per-segment evaluation, for every
/// filter and after every kind of change to the segments, and that the index is only used when it is safe to.
///
/// The indexed variant is allowed to differ from the scan in one way, by design: it does not keep candidates the pruner
/// has never been told about. Every test therefore uses candidates that the pruner has been told about, except the ones
/// that are about that difference.
public class PartitionIndexPruningTest {
  private static final String TABLE = "testTable_OFFLINE";
  private static final String COLUMN = "memberId";
  private static final String OTHER_COLUMN = "memberName";
  private static final int NUM_PARTITIONS = 8;
  private static final String MODULO = "Modulo";

  @Test
  public void testIndexedPruningReturnsTheMatchingSegments() throws Exception {
    Map<String, ZNRecord> records = new HashMap<>();
    records.put("p0", metadata("p0", Map.of(COLUMN, partitions(0))));
    records.put("p1", metadata("p1", Map.of(COLUMN, partitions(1))));
    records.put("p1p2", metadata("p1p2", Map.of(COLUMN, partitions(1, 2))));
    records.put("p3", metadata("p3", Map.of(COLUMN, partitions(3))));
    SinglePartitionColumnSegmentPruner pruner = singleColumnPruner(records);
    Set<String> all = records.keySet();

    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "1")), all), Set.of("p1", "p1p2"));
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "2")), all), Set.of("p1p2"));
    assertEquals(pruner.pruneAssumingSyncedScope(request(in(COLUMN, "0", "3")), all), Set.of("p0", "p3"));
    // 9 is partition 1
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "9")), all), Set.of("p1", "p1p2"));
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "5")), all), Set.of());
  }

  @Test
  public void testIndexedPruningOnlyReturnsCandidates() throws Exception {
    Map<String, ZNRecord> records = new HashMap<>();
    records.put("p1a", metadata("p1a", Map.of(COLUMN, partitions(1))));
    records.put("p1b", metadata("p1b", Map.of(COLUMN, partitions(1))));
    records.put("p2", metadata("p2", Map.of(COLUMN, partitions(2))));
    SinglePartitionColumnSegmentPruner pruner = singleColumnPruner(records);

    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "1")), Set.of("p1a", "p2")), Set.of("p1a"));
  }

  @Test
  public void testFilterThatDoesNotConstrainThePartitionColumnKeepsTheCandidates() throws Exception {
    Map<String, ZNRecord> records = new HashMap<>();
    records.put("p1", metadata("p1", Map.of(COLUMN, partitions(1))));
    records.put("p2", metadata("p2", Map.of(COLUMN, partitions(2))));
    SinglePartitionColumnSegmentPruner pruner = singleColumnPruner(records);
    Set<String> candidates = records.keySet();

    // No pruning is possible, so the candidates are handed back untouched
    assertSame(pruner.pruneAssumingSyncedScope(request(equalsTo(OTHER_COLUMN, "1")), candidates), candidates);
    // One branch of an OR that does not constrain the partition column keeps everything
    assertSame(pruner.pruneAssumingSyncedScope(
        request(or(equalsTo(COLUMN, "1"), equalsTo(OTHER_COLUMN, "1"))), candidates), candidates);
    // An AND is narrowed by the branches that do constrain it
    assertEquals(pruner.pruneAssumingSyncedScope(
        request(and(equalsTo(COLUMN, "1"), equalsTo(OTHER_COLUMN, "1"))), candidates), Set.of("p1"));
  }

  @Test
  public void testSegmentsWithoutUsableMetadataAreNeverPruned() throws Exception {
    Map<String, ZNRecord> records = new HashMap<>();
    records.put("p1", metadata("p1", Map.of(COLUMN, partitions(1))));
    records.put("p2", metadata("p2", Map.of(COLUMN, partitions(2))));
    // Valid ZK metadata that has no partition metadata, which the pruner can never learn more about
    records.put("noPartitionMetadata", new ZNRecord("noPartitionMetadata"));
    // The metadata of another column only
    records.put("otherColumnOnly", metadata("otherColumnOnly", Map.of(OTHER_COLUMN, partitions(5))));
    SinglePartitionColumnSegmentPruner pruner = singleColumnPruner(records);

    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "1")), records.keySet()),
        Set.of("p1", "noPartitionMetadata", "otherColumnOnly"));
  }

  @Test
  public void testSegmentWhoseMetadataIsNotAvailableYetIsKeptUntilItArrives() throws Exception {
    SinglePartitionColumnSegmentPruner pruner = new SinglePartitionColumnSegmentPruner(TABLE, COLUMN);
    pruner.init(null, null, List.of(), List.of());
    Set<String> online = new HashSet<>(Set.of("p1", "late"));

    // The ZK metadata of "late" cannot be read yet
    pruner.onAssignmentChange(null, null, online, List.of("p1", "late"),
        records(metadata("p1", Map.of(COLUMN, partitions(1))), null));
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "2")), online), Set.of("late"));

    // It arrives with a partition that the filter rules out
    pruner.onAssignmentChange(null, null, online, List.of("late"),
        records(metadata("late", Map.of(COLUMN, partitions(3)))));
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "2")), online), Set.of());
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "3")), online), Set.of("late"));
  }

  @Test
  public void testRefreshMovesASegmentBetweenPartitions() throws Exception {
    Map<String, ZNRecord> records = new HashMap<>();
    records.put("moving", metadata("moving", Map.of(COLUMN, partitions(1))));
    records.put("staying", metadata("staying", Map.of(COLUMN, partitions(1))));
    SinglePartitionColumnSegmentPruner pruner = singleColumnPruner(records);
    Set<String> online = records.keySet();

    pruner.refreshSegment("moving", metadata("moving", Map.of(COLUMN, partitions(2))));
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "1")), online), Set.of("staying"));
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "2")), online), Set.of("moving"));

    // A refresh that cannot read the metadata must not make the segment disappear
    pruner.refreshSegment("moving", null);
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "1")), online), Set.of("staying", "moving"));
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "7")), online), Set.of("moving"));
  }

  @Test
  public void testSegmentsThatAreNoLongerOnlineAreForgotten() throws Exception {
    Map<String, ZNRecord> records = new HashMap<>();
    records.put("kept", metadata("kept", Map.of(COLUMN, partitions(1))));
    records.put("gone", metadata("gone", Map.of(COLUMN, partitions(1))));
    SinglePartitionColumnSegmentPruner pruner = singleColumnPruner(records);

    pruner.onAssignmentChange(null, null, Set.of("kept"), List.of(), List.of());

    // A query that still lists the removed segment is not given it back by the index, which is why the caller has to
    // check that the candidates are in sync
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "1")), Set.of("kept", "gone")),
        Set.of("kept"));
    assertEquals(pruner.prune(request(equalsTo(COLUMN, "1")), Set.of("kept", "gone")), Set.of("kept", "gone"));
  }

  @Test
  public void testScanKeepsCandidatesThePrunerHasNeverHeardOf() throws Exception {
    Map<String, ZNRecord> records = new HashMap<>();
    records.put("p1", metadata("p1", Map.of(COLUMN, partitions(1))));
    SinglePartitionColumnSegmentPruner pruner = singleColumnPruner(records);
    Set<String> candidates = Set.of("p1", "stranger");

    assertEquals(pruner.prune(request(equalsTo(COLUMN, "2")), candidates), Set.of("stranger"));
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "2")), candidates), Set.of());
  }

  @Test
  public void testIndexIsNotUsedWhileASegmentUsesAFunctionThatCannotBeIndexed() throws Exception {
    Map<String, ZNRecord> records = new HashMap<>();
    records.put("modulo", metadata("modulo", Map.of(COLUMN, partitions(1))));
    // Legacy plugins do not opt into partition id preparation, so equal functions cannot be told apart
    records.put("legacy", metadata("legacy", Map.of(COLUMN,
        new ColumnPartitionMetadata("PrunerLegacy", NUM_PARTITIONS, Set.of(2), null))));
    SinglePartitionColumnSegmentPruner pruner = singleColumnPruner(records);
    Set<String> candidates = Set.of("modulo", "legacy", "stranger");

    // The scan is used, which is the one that keeps a candidate it has never heard of
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "2")), candidates),
        Set.of("legacy", "stranger"));

    // Once the segment is gone the index takes over, which drops the stranger
    pruner.onAssignmentChange(null, null, Set.of("modulo"), List.of(), List.of());
    assertEquals(pruner.pruneAssumingSyncedScope(request(equalsTo(COLUMN, "2")), candidates), Set.of());
  }

  @Test
  public void testFilterValueThePartitionFunctionRejectsBehavesAsTheScanDoes() throws Exception {
    Map<String, ZNRecord> records = new HashMap<>();
    records.put("p1", metadata("p1", Map.of(COLUMN, partitions(1))));
    SinglePartitionColumnSegmentPruner pruner = singleColumnPruner(records);
    BrokerRequest request = request(equalsTo(COLUMN, "not-a-number"));

    // The index fails on the value and defers to the scan, so the caller sees what the scan does
    expectThrows(NumberFormatException.class, () -> pruner.prune(request, records.keySet()));
    expectThrows(NumberFormatException.class, () -> pruner.pruneAssumingSyncedScope(request, records.keySet()));
  }

  @Test
  public void testIndexMatchesScanThroughoutARandomHistoryOfSingleColumnChanges() throws Exception {
    Random random = new Random(20260101L);
    SinglePartitionColumnSegmentPruner pruner = new SinglePartitionColumnSegmentPruner(TABLE, COLUMN);
    pruner.init(null, null, List.of(), List.of());
    Set<String> online = new HashSet<>();
    Set<String> unresolved = new HashSet<>();
    int nextSegment = 0;

    for (int step = 0; step < 600; step++) {
      switch (random.nextInt(5)) {
        case 0: {
          // New segments, some of which have no readable metadata yet
          List<String> pulled = new ArrayList<>();
          List<ZNRecord> pulledRecords = new ArrayList<>();
          for (int i = random.nextInt(4) + 1; i > 0; i--) {
            String segment = "segment_" + nextSegment++;
            online.add(segment);
            pulled.add(segment);
            if (random.nextInt(8) == 0) {
              unresolved.add(segment);
              pulledRecords.add(null);
            } else {
              pulledRecords.add(randomMetadata(random, segment, List.of(COLUMN)));
            }
          }
          pruner.onAssignmentChange(null, null, online, pulled, pulledRecords);
          break;
        }
        case 1: {
          // The metadata of unresolved segments becomes readable
          List<String> pulled = new ArrayList<>(unresolved);
          List<ZNRecord> pulledRecords = new ArrayList<>();
          for (String segment : pulled) {
            pulledRecords.add(randomMetadata(random, segment, List.of(COLUMN)));
          }
          unresolved.clear();
          pruner.onAssignmentChange(null, null, online, pulled, pulledRecords);
          break;
        }
        case 2: {
          // Segments go offline
          for (String segment : pickSome(random, online, 3)) {
            online.remove(segment);
            unresolved.remove(segment);
          }
          pruner.onAssignmentChange(null, null, online, List.of(), List.of());
          break;
        }
        default: {
          // A refresh, to other partitions, or to metadata that cannot be read
          for (String segment : pickSome(random, online, 2)) {
            if (random.nextInt(6) == 0) {
              unresolved.add(segment);
              pruner.refreshSegment(segment, null);
            } else {
              unresolved.remove(segment);
              pruner.refreshSegment(segment, randomMetadata(random, segment, List.of(COLUMN)));
            }
          }
          break;
        }
      }
      assertIndexMatchesScan(random, pruner, online, singleColumnFilters());
    }
  }

  @Test
  public void testIndexMatchesScanThroughoutARandomHistoryOfMultiColumnChanges() throws Exception {
    Random random = new Random(20260102L);
    MultiPartitionColumnsSegmentPruner pruner =
        new MultiPartitionColumnsSegmentPruner(TABLE, Set.of(COLUMN, OTHER_COLUMN));
    pruner.init(null, null, List.of(), List.of());
    Set<String> online = new HashSet<>();
    Set<String> unresolved = new HashSet<>();
    int nextSegment = 0;

    for (int step = 0; step < 600; step++) {
      switch (random.nextInt(5)) {
        case 0: {
          List<String> pulled = new ArrayList<>();
          List<ZNRecord> pulledRecords = new ArrayList<>();
          for (int i = random.nextInt(4) + 1; i > 0; i--) {
            String segment = "segment_" + nextSegment++;
            online.add(segment);
            pulled.add(segment);
            if (random.nextInt(8) == 0) {
              unresolved.add(segment);
              pulledRecords.add(null);
            } else {
              pulledRecords.add(randomMetadata(random, segment, List.of(COLUMN, OTHER_COLUMN)));
            }
          }
          pruner.onAssignmentChange(null, null, online, pulled, pulledRecords);
          break;
        }
        case 1: {
          List<String> pulled = new ArrayList<>(unresolved);
          List<ZNRecord> pulledRecords = new ArrayList<>();
          for (String segment : pulled) {
            pulledRecords.add(randomMetadata(random, segment, List.of(COLUMN, OTHER_COLUMN)));
          }
          unresolved.clear();
          pruner.onAssignmentChange(null, null, online, pulled, pulledRecords);
          break;
        }
        case 2: {
          for (String segment : pickSome(random, online, 3)) {
            online.remove(segment);
            unresolved.remove(segment);
          }
          pruner.onAssignmentChange(null, null, online, List.of(), List.of());
          break;
        }
        default: {
          for (String segment : pickSome(random, online, 2)) {
            if (random.nextInt(6) == 0) {
              unresolved.add(segment);
              pruner.refreshSegment(segment, null);
            } else {
              unresolved.remove(segment);
              pruner.refreshSegment(segment, randomMetadata(random, segment, List.of(COLUMN, OTHER_COLUMN)));
            }
          }
          break;
        }
      }
      assertIndexMatchesScan(random, pruner, online, multiColumnFilters());
    }
  }

  @Test
  public void testApplyingNoChangeReturnsTheSameIndex() {
    PartitionFunction function = PartitionFunctionFactory.getPartitionFunction(MODULO, NUM_PARTITIONS, null);
    Map<String, SegmentPartitionInfo> infos = Map.of(COLUMN, new SegmentPartitionInfo(COLUMN, function, Set.of(1)));
    PartitionIndex index = PartitionIndex.empty(Set.of(COLUMN))
        .applyChanges(List.of(new PartitionIndex.SegmentChange("segment", null, infos)));

    // Same partitions, as when a committed segment is refreshed with the metadata it already had
    Map<String, SegmentPartitionInfo> sameInfos =
        Map.of(COLUMN, new SegmentPartitionInfo(COLUMN, function, Set.of(1)));
    assertSame(index.applyChanges(List.of(new PartitionIndex.SegmentChange("segment", infos, sameInfos))), index);
    assertSame(index.applyChanges(List.of()), index);
    // Not indexed before and after
    assertSame(index.applyChanges(List.of(new PartitionIndex.SegmentChange("other", null, null))), index);
  }

  @Test
  public void testChangesNeverAffectAnIndexThatWasAlreadyPublished() {
    PartitionFunction function = PartitionFunctionFactory.getPartitionFunction(MODULO, NUM_PARTITIONS, null);
    Map<String, SegmentPartitionInfo> infos = Map.of(COLUMN, new SegmentPartitionInfo(COLUMN, function, Set.of(1)));
    PartitionIndex before = PartitionIndex.empty(Set.of(COLUMN))
        .applyChanges(List.of(new PartitionIndex.SegmentChange("segment", null, infos)));
    Expression filter = equalsTo(COLUMN, "1");

    PartitionIndex after = before.applyChanges(List.of(new PartitionIndex.SegmentChange("segment", infos, null),
        new PartitionIndex.SegmentChange("another", null, infos)));

    assertEquals(before.resolve(filter, Set.of("segment", "another")), Set.of("segment"));
    assertEquals(after.resolve(filter, Set.of("segment", "another")), Set.of("another"));
    assertFalse(before.hasUnindexableSegments());
  }

  @Test
  public void testIndexRecordsSegmentsWhoseFunctionCannotBeIndexed() {
    PartitionFunction legacy = PartitionFunctionFactory.getPartitionFunction("PrunerLegacy", NUM_PARTITIONS, null);
    Map<String, SegmentPartitionInfo> infos = Map.of(COLUMN, new SegmentPartitionInfo(COLUMN, legacy, Set.of(1)));
    PartitionIndex index = PartitionIndex.empty(Set.of(COLUMN))
        .applyChanges(List.of(new PartitionIndex.SegmentChange("segment", null, infos)));
    assertTrue(index.hasUnindexableSegments());

    index = index.applyChanges(List.of(new PartitionIndex.SegmentChange("segment", infos, null)));
    assertFalse(index.hasUnindexableSegments());
  }

  private static void assertIndexMatchesScan(Random random, SegmentPruner pruner, Set<String> online,
      List<Expression> filters) {
    List<Set<String>> candidateSets = new ArrayList<>();
    candidateSets.add(online);
    candidateSets.add(pickSome(random, online, Math.max(1, online.size() / 3)));
    for (Expression filter : filters) {
      BrokerRequest request = request(filter);
      for (Set<String> candidates : candidateSets) {
        assertEquals(pruner.pruneAssumingSyncedScope(request, candidates), pruner.prune(request, candidates),
            "Filter: " + filter);
      }
    }
  }

  private static List<Expression> singleColumnFilters() {
    List<Expression> filters = new ArrayList<>();
    for (int value = 0; value < NUM_PARTITIONS + 2; value++) {
      filters.add(equalsTo(COLUMN, Integer.toString(value)));
    }
    filters.add(in(COLUMN, "1", "3", "9"));
    filters.add(in(COLUMN, "2"));
    filters.add(in(COLUMN, "0", "1", "2", "3", "4", "5", "6", "7"));
    filters.add(and(equalsTo(COLUMN, "1"), in(COLUMN, "1", "2")));
    filters.add(and(equalsTo(COLUMN, "1"), equalsTo(COLUMN, "2")));
    filters.add(and(equalsTo(COLUMN, "1"), equalsTo(OTHER_COLUMN, "2")));
    filters.add(or(equalsTo(COLUMN, "1"), equalsTo(COLUMN, "4")));
    filters.add(or(equalsTo(COLUMN, "1"), equalsTo(OTHER_COLUMN, "4")));
    filters.add(equalsTo(OTHER_COLUMN, "4"));
    filters.add(or(and(equalsTo(COLUMN, "1"), in(COLUMN, "1", "5")), equalsTo(COLUMN, "6")));
    return filters;
  }

  private static List<Expression> multiColumnFilters() {
    List<Expression> filters = singleColumnFilters();
    for (int value = 0; value < NUM_PARTITIONS; value += 3) {
      filters.add(equalsTo(OTHER_COLUMN, Integer.toString(value)));
    }
    filters.add(in(OTHER_COLUMN, "1", "2"));
    filters.add(and(equalsTo(COLUMN, "1"), equalsTo(OTHER_COLUMN, "2")));
    filters.add(and(in(COLUMN, "1", "2"), in(OTHER_COLUMN, "2", "3")));
    filters.add(or(equalsTo(COLUMN, "1"), equalsTo(OTHER_COLUMN, "2")));
    filters.add(or(and(equalsTo(COLUMN, "1"), equalsTo(OTHER_COLUMN, "2")), equalsTo(OTHER_COLUMN, "5")));
    filters.add(and(or(equalsTo(COLUMN, "1"), equalsTo(OTHER_COLUMN, "2")), equalsTo(COLUMN, "1")));
    return filters;
  }

  private static Set<String> pickSome(Random random, Set<String> segments, int max) {
    List<String> shuffled = new ArrayList<>(segments);
    shuffled.sort(null);
    for (int i = shuffled.size() - 1; i > 0; i--) {
      int j = random.nextInt(i + 1);
      String swap = shuffled.get(i);
      shuffled.set(i, shuffled.get(j));
      shuffled.set(j, swap);
    }
    return new HashSet<>(shuffled.subList(0, Math.min(shuffled.size(), random.nextInt(max + 1))));
  }

  /// Metadata for the given columns, where each column has one or two partitions or is left out, and one segment in
  /// twelve has no partition metadata at all.
  private static ZNRecord randomMetadata(Random random, String segment, List<String> columns)
      throws Exception {
    if (random.nextInt(12) == 0) {
      return new ZNRecord(segment);
    }
    Map<String, ColumnPartitionMetadata> columnMetadata = new HashMap<>();
    for (String column : columns) {
      if (random.nextInt(6) != 0) {
        Set<Integer> partitions = new HashSet<>();
        partitions.add(random.nextInt(NUM_PARTITIONS));
        if (random.nextInt(4) == 0) {
          partitions.add(random.nextInt(NUM_PARTITIONS));
        }
        columnMetadata.put(column, new ColumnPartitionMetadata(MODULO, NUM_PARTITIONS, partitions, null));
      }
    }
    return metadata(segment, columnMetadata);
  }

  private static SinglePartitionColumnSegmentPruner singleColumnPruner(Map<String, ZNRecord> records) {
    SinglePartitionColumnSegmentPruner pruner = new SinglePartitionColumnSegmentPruner(TABLE, COLUMN);
    pruner.init(null, null, new ArrayList<>(records.keySet()), new ArrayList<>(records.values()));
    return pruner;
  }

  private static ColumnPartitionMetadata partitions(Integer... partitions) {
    return new ColumnPartitionMetadata(MODULO, NUM_PARTITIONS, Set.of(partitions), null);
  }

  private static ZNRecord metadata(String segment, Map<String, ColumnPartitionMetadata> columnMetadata)
      throws Exception {
    ZNRecord record = new ZNRecord(segment);
    record.setSimpleField(CommonConstants.Segment.PARTITION_METADATA,
        new SegmentPartitionMetadata(columnMetadata).toJsonString());
    return record;
  }

  private static List<ZNRecord> records(@Nullable ZNRecord... records) {
    List<ZNRecord> list = new ArrayList<>();
    for (ZNRecord record : records) {
      list.add(record);
    }
    return list;
  }

  private static Expression equalsTo(String column, String value) {
    return RequestUtils.getFunctionExpression("EQUALS",
        List.of(RequestUtils.getIdentifierExpression(column), RequestUtils.getLiteralExpression(value)));
  }

  private static Expression in(String column, String... values) {
    List<Expression> operands = new ArrayList<>();
    operands.add(RequestUtils.getIdentifierExpression(column));
    for (String value : values) {
      operands.add(RequestUtils.getLiteralExpression(value));
    }
    return RequestUtils.getFunctionExpression("IN", operands);
  }

  private static Expression and(Expression... operands) {
    return RequestUtils.getFunctionExpression("AND", List.of(operands));
  }

  private static Expression or(Expression... operands) {
    return RequestUtils.getFunctionExpression("OR", List.of(operands));
  }

  private static BrokerRequest request(Expression filter) {
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setFilterExpression(filter);
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setPinotQuery(pinotQuery);
    return brokerRequest;
  }
}
