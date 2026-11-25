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
package org.apache.pinot.broker.routing.manager;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class SegmentAssignmentEpochTest {

  @Test
  public void testReadWithNoUpdateIsUnchanged() {
    SegmentAssignmentEpoch epoch = new SegmentAssignmentEpoch();

    long token = epoch.startRead();

    assertTrue(epoch.isUnchanged(token));
    // Reads do not disturb each other
    assertTrue(epoch.isUnchanged(epoch.startRead()));
    assertTrue(epoch.isUnchanged(token));
  }

  @Test
  public void testReadSpanningACompletedUpdateIsChanged() {
    SegmentAssignmentEpoch epoch = new SegmentAssignmentEpoch();
    long token = epoch.startRead();

    epoch.beginUpdate();
    epoch.endUpdate();

    assertFalse(epoch.isUnchanged(token));
  }

  @Test
  public void testReadSpanningARunningUpdateIsChanged() {
    SegmentAssignmentEpoch epoch = new SegmentAssignmentEpoch();
    long token = epoch.startRead();

    epoch.beginUpdate();

    assertFalse(epoch.isUnchanged(token));
  }

  @Test
  public void testReadSpanningSeveralUpdatesIsChanged() {
    SegmentAssignmentEpoch epoch = new SegmentAssignmentEpoch();
    long token = epoch.startRead();

    for (int i = 0; i < 3; i++) {
      epoch.beginUpdate();
      epoch.endUpdate();
    }

    assertFalse(epoch.isUnchanged(token));
  }

  @Test
  public void testReadStartedDuringAnUpdateIsNeverClean() {
    SegmentAssignmentEpoch epoch = new SegmentAssignmentEpoch();
    epoch.beginUpdate();

    long token = epoch.startRead();

    assertEquals(token, SegmentAssignmentEpoch.UNSTABLE);
    assertFalse(epoch.isUnchanged(token));
    // Not even once the update is over, because the read began against a half-applied change
    epoch.endUpdate();
    assertFalse(epoch.isUnchanged(token));
  }

  @Test
  public void testReadStartedAfterAnUpdateIsClean() {
    SegmentAssignmentEpoch epoch = new SegmentAssignmentEpoch();
    epoch.beginUpdate();
    epoch.endUpdate();

    long token = epoch.startRead();

    assertTrue(epoch.isUnchanged(token));
  }

  /// If an update throws, its caller never calls `endUpdate()`. Readers must keep taking the safe path until a later
  /// update completes, including when that later update begins while the epoch is already marked as in progress.
  @Test
  public void testFailedUpdateKeepsReadsUnstableUntilALaterUpdateCompletes() {
    SegmentAssignmentEpoch epoch = new SegmentAssignmentEpoch();
    epoch.beginUpdate();

    assertEquals(epoch.startRead(), SegmentAssignmentEpoch.UNSTABLE);

    epoch.beginUpdate();
    assertEquals(epoch.startRead(), SegmentAssignmentEpoch.UNSTABLE);

    epoch.endUpdate();
    long token = epoch.startRead();
    assertTrue(epoch.isUnchanged(token));
  }
}
