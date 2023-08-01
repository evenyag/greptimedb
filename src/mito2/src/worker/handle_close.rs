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

//! Handling close request.

use common_telemetry::info;

use crate::error::Result;
use crate::worker::request::CloseRequest;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_close_request(&mut self, request: CloseRequest) -> Result<()> {
        let Some(region) = self.regions.get_region(request.region_id) else {
            return Ok(());
        };

        info!("Try to close region {}", request.region_id);

        region.stop().await?;
        self.regions.remove_region(request.region_id);

        info!("Region {} closed", request.region_id);

        Ok(())
    }
}
