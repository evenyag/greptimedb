// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BinaryHeap;

/// A comparable node of the heap.
trait NodeCmp: Eq + Ord {
    /// Returns whether the node still has batch to read.
    fn is_eof(&self) -> bool;

    /// Returns true if the key range of current batch in `self` is behind (exclusive) current
    /// batch in `other`.
    ///
    /// # Panics
    /// Panics if either `self` or `other` is EOF.
    fn is_behind(&self, other: &Self) -> bool;
}

/// Common algorithm of merging sorted batches from multiple nodes.
struct MergeAlgo<T> {
    /// Holds nodes whose key range of current batch **is** overlapped with the merge window.
    /// Each node yields batches from a `source`.
    ///
    /// Node in this heap **must** not be empty. A `merge window` is the (primary key, timestamp)
    /// range of the **root node** in the `hot` heap.
    hot: BinaryHeap<T>,
    /// Holds nodes whose key range of current batch **isn't** overlapped with the merge window.
    ///
    /// Nodes in this heap **must** not be empty.
    cold: BinaryHeap<T>,
}

impl<T: NodeCmp> MergeAlgo<T> {
    /// Creates a new merge algorithm from `nodes`.
    ///
    /// All nodes must be initialized.
    fn new(mut nodes: Vec<T>) -> Self {
        // Skips EOF nodes.
        nodes.retain(|node| !node.is_eof());
        let hot = BinaryHeap::with_capacity(nodes.len());
        let cold = BinaryHeap::from(nodes);

        let mut algo = MergeAlgo { hot, cold };
        // Initializes the algorithm.
        algo.refill_hot();

        algo
    }

    /// Moves nodes in `cold` heap, whose key range is overlapped with current merge
    /// window to `hot` heap.
    fn refill_hot(&mut self) {
        while !self.cold.is_empty() {
            if let Some(merge_window) = self.hot.peek() {
                let warmest = self.cold.peek().unwrap();
                if warmest.is_behind(merge_window) {
                    // if the warmest node in the `cold` heap is totally after the
                    // `merge_window`, then no need to add more nodes into the `hot`
                    // heap for merge sorting.
                    break;
                }
            }

            let warmest = self.cold.pop().unwrap();
            self.hot.push(warmest);
        }
    }

    /// Push the node popped from `hot` back to a proper heap.
    fn reheap(&mut self, node: T) {
        if node.is_eof() {
            // If the node is EOF, don't put it into the heap again.
            // The merge window would be updated, need to refill the hot heap.
            self.refill_hot();
        } else {
            // Find a proper heap for this node.
            let node_is_cold = if let Some(hottest) = self.hot.peek() {
                // If key range of this node is behind the hottest node's then we can
                // push it to the cold heap. Otherwise we should push it to the hot heap.
                node.is_behind(hottest)
            } else {
                // The hot heap is empty, but we don't known whether the current
                // batch of this node is still the hottest.
                true
            };

            if node_is_cold {
                self.cold.push(node);
            } else {
                self.hot.push(node);
            }
            // Anyway, the merge window has been changed, we need to refill the hot heap.
            self.refill_hot();
        }
    }
}
