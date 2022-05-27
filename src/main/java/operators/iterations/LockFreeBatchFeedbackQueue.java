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
package operators.iterations;

import org.apache.flink.statefun.flink.core.queue.Locks;
import org.apache.flink.statefun.flink.core.queue.MpscQueue;

import java.util.Deque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One implementation of FeedbackQueue
 * @param <ElementT> Element that should be sent using this queue
 */
public final class LockFreeBatchFeedbackQueue<ElementT> {
  private static final int INITIAL_BUFFER_SIZE = 32 * 1024; // 32k
  private final AtomicBoolean isActive = new AtomicBoolean(true);
  private final MpscQueue<ElementT> queue = new MpscQueue<>(INITIAL_BUFFER_SIZE, Locks.spinLock());

  public boolean addAndCheckIfWasEmpty(ElementT element) {
    final int size = queue.add(element);
    return size == 1;
  }

  public void setIsActive(boolean isActive) {
    this.isActive.set(isActive);
  }

  public boolean getIsActive() {
    return isActive.getPlain();
  }

  public Deque<ElementT> drainAll() {
    return queue.drainAll();
  }
}
