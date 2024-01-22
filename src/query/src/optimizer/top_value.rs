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

use datafusion_common::tree_node::{TreeNodeVisitor, VisitRecursion};
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::{OptimizerRule, OptimizerConfig};
use datafusion_common::Result;

/// This rule pushes down `last_value`/`first_value` aggregator as a hint to the
/// leaf table scan node.
pub struct TopValuePushDownRule;

impl OptimizerRule for TopValuePushDownRule {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        todo!()
    }

    fn name(&self) -> &str {
        "TopValuePushDownRule"
    }
}

impl TopValuePushDownRule {
    //
}

/// Find the most closest top value aggregator to the leaf node.
#[derive(Default)]
struct TopValueVisitor {
}

impl TreeNodeVisitor for TopValueVisitor {
    type N = LogicalPlan;

    fn pre_visit(&mut self, node: &Self::N) -> Result<VisitRecursion> {
        // if let LogicalPlan::Sort(sort) = node {
        //     let mut exprs = vec![];
        //     for expr in &sort.expr {
        //         if let Expr::Sort(sort_expr) = expr {
        //             exprs.push(sort_expr.clone());
        //         }
        //     }
        //     self.order_expr = Some(exprs);
        // }

        // if let LogicalPlan::Aggregate(aggr) = node {
        //     //
        // }

        Ok(VisitRecursion::Continue)
    }
}
