// Copyright (c) 2019-2026 Provable Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use snarkos_utilities::locks::parking_lot::Mutex;

#[derive(Default)]
struct SyncMetricsData {
    /// The number of block requests completed since the last update.
    completed_requests: VecDeque<Instant>,

    /// The current sync speed
    sync_speed: f64,
}

#[derive(Default)]
pub struct BlockSyncMetrics {
    data: Mutex<SyncMetricsData>,
}

impl BlockSyncMetrics {
    /// Sync speed is calculated on a sliding window.
    const METRIC_WINDOW: Duration = Duration::from_secs(60);

    /// Updates the sync speed and returns the new value.
    pub fn get_sync_speed(&self) -> f64 {
        let mut data = self.data.lock();

        // Remove requests that are past the sliding window.
        while let Some(time) = data.completed_requests.front()
            && time.elapsed() > Self::METRIC_WINDOW
        {
            data.completed_requests.pop_front();
        }

        // Update sync speed based on the last minute.
        data.sync_speed = data.completed_requests.len() as f64 / Self::METRIC_WINDOW.as_secs_f64();

        data.sync_speed
    }

    pub fn count_request_completed(&self) {
        let mut data = self.data.lock();

        // Remove requests that are past the sliding window.
        while let Some(time) = data.completed_requests.front()
            && time.elapsed() > Self::METRIC_WINDOW
        {
            data.completed_requests.pop_front();
        }

        // Add time for the new request.
        data.completed_requests.push_back(Instant::now());
    }

    pub fn mark_fully_synced(&self) {
        // Set speed to zero because it otherwise only gets updated during sync.
        // Keep request data, in case we resume syncing.
        self.data.lock().sync_speed = 0.0;
    }
}
