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

import java.util.concurrent.atomic.AtomicLong;


/// Tells a query whether a segment assignment update overlapped with the time it spent selecting and pruning segments.
///
/// A routing entry applies an assignment change to its components one after another: the segment pruners first, then
/// the segment selector. A query that selected its segments before the change can therefore reach a pruner that has
/// already applied it, and see segments the pruner has since forgotten. Pruners that cannot tolerate that (see
/// `SegmentPruner#pruneAssumingSyncedScope`) are only used when this class proves that no update overlapped, and the
/// result is thrown away and recomputed the safe way when one did.
///
/// This is a sequence lock. The counter is odd while an update is in progress and even otherwise, and it only ever
/// increases:
/// ```
/// writer:  beginUpdate() -> counter odd    ... apply the change ...    endUpdate() -> counter even, larger
/// reader:  token = startRead()    ... select and prune ...    isUnchanged(token)
/// ```
/// A read is clean only if it started while the counter was even and the counter is still the same value afterwards.
/// Because the counter never returns to a previous value, an update that started and finished in between is detected
/// as well as one that is still running.
///
/// If an update fails halfway, its caller must not call [#endUpdate()]. The counter then stays odd, so readers keep
/// taking the safe path until a later update completes, because the components may be out of step with each other
/// until then.
///
/// Thread-safety: all methods are safe to call from any thread. Updates for one routing entry are already serialized
/// by the caller (they run under the table lock).
final class SegmentAssignmentEpoch {
  /// Returned by [#startRead()] when an update is in progress. It is never equal to a counter value.
  static final long UNSTABLE = -1L;

  private final AtomicLong _counter = new AtomicLong();

  /// Marks the start of an update. Idempotent if a previous update failed without calling [#endUpdate()].
  void beginUpdate() {
    _counter.updateAndGet(counter -> counter | 1L);
  }

  /// Marks the end of a successful update.
  void endUpdate() {
    _counter.updateAndGet(counter -> (counter | 1L) + 1L);
  }

  /// Returns a token to pass to [#isUnchanged(long)] once the read is done, or [#UNSTABLE] if an update is in progress
  /// (in which case the read can never be clean).
  long startRead() {
    long counter = _counter.get();
    return (counter & 1L) == 0L ? counter : UNSTABLE;
  }

  /// Returns `true` if no update started, finished or is running since the read that produced `token` began.
  boolean isUnchanged(long token) {
    return token != UNSTABLE && _counter.get() == token;
  }
}
