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

use datafusion::datasource::DefaultTableSource;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion_common::Result;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::{aggregate_function, Expr, LogicalPlan};
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use store_api::storage::{TopHint, TopType};

use crate::dummy_catalog::DummyTableProvider;

/// This rule pushes down `last_value`/`first_value` aggregator as a hint to the
/// leaf table scan node.
pub struct TopValuePushDownRule;

impl OptimizerRule for TopValuePushDownRule {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let mut visitor = TopValueVisitor::default();
        plan.visit(&mut visitor)?;

        if let Some(is_last) = visitor.is_last {
            let new_plan = plan.clone();
            let new_plan = new_plan.transform_down(&|plan| {
                Self::set_top_value_hint(plan, &visitor.group_expr, is_last)
            })?;

            Ok(Some(new_plan))
        } else {
            Ok(Some(plan.clone()))
        }
    }

    fn name(&self) -> &str {
        "TopValuePushDownRule"
    }
}

impl TopValuePushDownRule {
    fn set_top_value_hint(
        plan: LogicalPlan,
        group_expr: &[Expr],
        is_last: bool,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::TableScan(table_scan) = &plan else {
            return Ok(Transformed::No(plan));
        };

        let Some(source) = table_scan
            .source
            .as_any()
            .downcast_ref::<DefaultTableSource>()
        else {
            return Ok(Transformed::No(plan));
        };

        let Some(adapter) = source
            .table_provider
            .as_any()
            .downcast_ref::<DummyTableProvider>()
        else {
            return Ok(Transformed::No(plan));
        };

        // TODO(yingwen): Get group column names.

        if is_last {
            adapter.with_top_hint(TopHint {
                top_type: TopType::LastValue,
                group_columns: Vec::new(),
            });

            Ok(Transformed::Yes(plan))
        } else {
            Ok(Transformed::No(plan))
        }
    }
}

/// Find the most closest top value aggregator to the leaf node.
#[derive(Default)]
struct TopValueVisitor {
    group_expr: Vec<Expr>,
    is_last: Option<bool>,
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

        if let LogicalPlan::Aggregate(aggregate) = node {
            common_telemetry::info!("group is {:?}", aggregate.group_expr);
            // TODO(yingwen): Support first value.
            for expr in &aggregate.aggr_expr {
                let Expr::AggregateFunction(func) = expr else {
                    self.group_expr.clear();
                    self.is_last = None;
                    break;
                };
                common_telemetry::info!("func is {:?}", func);
                match func {
                    AggregateFunction {
                        fun: aggregate_function::AggregateFunction::LastValue,
                        args: _,
                        distinct: false,
                        filter: None,
                        order_by: None,
                    } => {
                        // TODO(yingwen): check args.
                        self.is_last = Some(true);
                    }
                    _ => {
                        self.group_expr.clear();
                        self.is_last = None;
                        break;
                    }
                }

                // if let AggregateFunction::LastValue = func.fun {
                //     // TODO(yingwen): Check other
                //     self.is_last = Some(true);
                // } else {
                //     self.group_expr.clear();
                //     self.is_last = None;
                //     break;
                // }
            }
            if self.is_last.is_some() {
                self.group_expr = aggregate.group_expr.clone();
            }
        }

        Ok(VisitRecursion::Continue)
    }
}
