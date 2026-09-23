//! Prospective per-port admission and ownership of deferred graph input.

use laminar_core::streaming::retained_arrow_bytes;

use super::{
    Arc, BackpressurePolicy, DbError, FxHashMap, GateDecision, InputFrontier, OperatorGraph,
    Ordering, RecordBatch, SourceBatchView,
};

pub(super) fn retained_input_bytes(batches: &[RecordBatch]) -> usize {
    batches.iter().fold(0usize, |total, batch| {
        total.saturating_add(retained_arrow_bytes(batch))
    })
}

impl OperatorGraph {
    fn port_usage_fits(&self, batches: usize, bytes: usize) -> bool {
        (self.max_input_buf_batches == 0 || batches <= self.max_input_buf_batches)
            && self.max_input_buf_bytes.is_none_or(|max| bytes <= max)
            && bytes < usize::MAX
            && batches < usize::MAX
    }

    fn projected_port_usage(
        &self,
        target: usize,
        port: u8,
        batches: usize,
        bytes: usize,
    ) -> (usize, usize) {
        let port = usize::from(port);
        (
            self.input_bufs[target][port].len().saturating_add(batches),
            self.input_buf_bytes[target][port].saturating_add(bytes),
        )
    }

    fn require_port_capacity(
        &self,
        target: usize,
        port: u8,
        batches: usize,
        bytes: usize,
    ) -> Result<(), DbError> {
        if self.backpressure_policy == BackpressurePolicy::ShedOldest {
            return Ok(());
        }
        let (batches, bytes) = self.projected_port_usage(target, port, batches, bytes);
        if self.port_usage_fits(batches, bytes) {
            return Ok(());
        }
        // The producer may already have changed state. No retry or checkpoint may reuse this
        // generation; the supervisor retains the terminal disposition across recovery attempts.
        self.execution_poisoned.store(true, Ordering::Release);
        self.poison_after_terminal_error();
        Err(DbError::GraphBufferBudgetExceeded {
            node: self.nodes[target].name.to_string(),
            port,
            batches,
            bytes,
            max_batches: self.max_input_buf_batches,
            max_bytes: self.max_input_buf_bytes,
        })
    }

    pub(super) fn preflight_output(
        &self,
        node: usize,
        batches: usize,
        bytes: usize,
    ) -> Result<(), DbError> {
        // Each destination owns its full charge even when Arrow backing storage is shared.
        // Duplicate edge registration is normalized when topology is built.
        for &(target, port) in &self.nodes[node].output_routes {
            self.require_port_capacity(target, port, batches, bytes)?;
        }
        Ok(())
    }

    pub(super) fn is_downstream_at_capacity(&self, node_id: usize) -> bool {
        let cap = self.max_input_buf_batches;
        let max_bytes = self.max_input_buf_bytes;
        if cap == 0 && max_bytes.is_none() {
            return false;
        }
        let routes = &self.nodes[node_id].output_routes;
        let source_input =
            self.source_node_ids.contains(&node_id) && !self.input_bufs[node_id][0].is_empty();
        routes.iter().any(|&(target, port)| {
            let p = usize::from(port);
            if source_input {
                // A source passthrough has an exact output size. Defer before consuming it when
                // a partially filled destination cannot fit, preserving its input and cursor.
                let (batches, bytes) = self.projected_port_usage(
                    target,
                    port,
                    self.input_bufs[node_id][0].len(),
                    self.input_buf_bytes[node_id][0],
                );
                !self.port_usage_fits(batches, bytes)
            } else {
                (cap > 0 && self.input_bufs[target][p].len() >= cap)
                    || max_bytes.is_some_and(|max| self.input_buf_bytes[target][p] >= max)
            }
        })
    }

    pub(super) fn gate_decision(&self, node_id: usize) -> GateDecision {
        if !self.is_downstream_at_capacity(node_id) {
            return GateDecision::Run;
        }
        match self.backpressure_policy {
            BackpressurePolicy::Backpressure => GateDecision::Skip,
            BackpressurePolicy::Fail => GateDecision::Fail,
            BackpressurePolicy::ShedOldest => GateDecision::Run,
        }
    }

    pub(super) fn push_to_port(
        &mut self,
        target: usize,
        port: u8,
        mut batches: Vec<RecordBatch>,
        mut bytes: usize,
    ) {
        if self.backpressure_policy == BackpressurePolicy::ShedOldest {
            bytes = self.shed_before_admission(target, port, &mut batches, bytes);
        }
        let buf = &mut self.input_bufs[target][usize::from(port)];
        if buf.is_empty() {
            *buf = batches;
        } else {
            buf.extend(batches);
        }
        self.input_buf_bytes[target][usize::from(port)] += bytes;
    }

    fn shed_before_admission(
        &mut self,
        target: usize,
        port: u8,
        incoming: &mut Vec<RecordBatch>,
        incoming_bytes: usize,
    ) -> usize {
        let p = usize::from(port);
        let buf = &self.input_bufs[target][p];
        let (mut count, mut bytes) =
            self.projected_port_usage(target, port, incoming.len(), incoming_bytes);
        let mut drop_n = 0;
        let mut rows = 0usize;
        for batch in buf.iter().chain(incoming.iter()) {
            if self.port_usage_fits(count, bytes) {
                break;
            }
            count -= 1;
            bytes = bytes.saturating_sub(retained_arrow_bytes(batch));
            rows = rows.saturating_add(batch.num_rows());
            drop_n += 1;
        }
        let drop_old = drop_n.min(buf.len());
        let old_bytes = retained_input_bytes(&buf[..drop_old]);
        self.input_bufs[target][p].drain(..drop_old);
        self.input_buf_bytes[target][p] -= old_bytes;
        incoming.drain(..drop_n - drop_old);
        if rows > 0 {
            if let Some(prom) = &self.prom {
                prom.shed_records_total
                    .with_label_values(&[&self.nodes[target].name])
                    .inc_by(rows as u64);
            }
        }
        bytes - self.input_buf_bytes[target][p]
    }

    pub(super) fn prime_sources(
        &mut self,
        source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        visible_source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
        current_watermark: i64,
        source_frontiers: Option<&FxHashMap<Arc<str>, InputFrontier>>,
    ) -> Result<(), DbError> {
        for route in &self.source_list {
            let batches = match route.view {
                SourceBatchView::Visible => visible_source_batches.get(&route.name),
                SourceBatchView::Positioned => source_batches.get(&route.name),
            };
            if let Some(batches) = batches.filter(|batches| !batches.is_empty()) {
                self.require_port_capacity(
                    route.node_id,
                    0,
                    batches.len(),
                    retained_input_bytes(batches),
                )?;
            }
        }
        for index in 0..self.source_list.len() {
            let route = &self.source_list[index];
            let node = route.node_id;
            let batches = match route.view {
                SourceBatchView::Visible => visible_source_batches.get(&route.name),
                SourceBatchView::Positioned => source_batches.get(&route.name),
            };
            if let Some(batches) = batches.filter(|batches| !batches.is_empty()) {
                self.push_to_port(node, 0, batches.clone(), retained_input_bytes(batches));
            }
            let name = &self.source_list[index].name;
            let frontier = source_frontiers
                .and_then(|frontiers| frontiers.get(name).copied())
                .unwrap_or_else(|| InputFrontier::from_watermark(current_watermark));
            self.source_input_frontiers[node] = frontier;
            if !self.node_has_buffered_input(node) {
                self.propagate_operator_frontier(node, &[], current_watermark);
            }
        }
        Ok(())
    }

    pub(super) fn operator_input_frontiers(
        &self,
        node_id: usize,
        current_watermark: i64,
    ) -> smallvec::SmallVec<[InputFrontier; 2]> {
        #[cfg(feature = "cluster")]
        use crate::operator::capability::ManagedStateContract;
        let port_count = self.nodes[node_id].input_port_count;
        #[cfg(feature = "cluster")]
        let managed_state = self.nodes[node_id].capability.managed_state;
        #[cfg(feature = "cluster")]
        let use_local_source_frontier = self.cluster_shuffle.is_some()
            && matches!(
                managed_state,
                Some(
                    ManagedStateContract::CoreWindowV1
                        | ManagedStateContract::SqlAggregateV1
                        | ManagedStateContract::BoundedIntervalJoinV3
                        | ManagedStateContract::TemporalJoinV1
                )
            );
        #[cfg(feature = "cluster")]
        let use_temporal_source_frontier =
            matches!(managed_state, Some(ManagedStateContract::TemporalJoinV1));
        (0..port_count)
            .map(|port| {
                let upstream = self.input_sources[node_id][port];
                #[cfg(feature = "cluster")]
                if use_local_source_frontier && self.source_node_ids.contains(&upstream) {
                    if use_temporal_source_frontier {
                        return self.hold_buffered_source_frontier(
                            upstream,
                            self.temporal_source_frontiers
                                .get(&upstream)
                                .copied()
                                .unwrap_or(self.local_source_frontiers[upstream]),
                        );
                    }
                    return self.hold_buffered_source_frontier(
                        upstream,
                        self.local_source_frontiers[upstream],
                    );
                }
                if upstream < self.output_watermarks.len() {
                    InputFrontier {
                        watermark: (self.output_watermarks[upstream] != i64::MIN)
                            .then_some(self.output_watermarks[upstream]),
                        idle: self.output_idle[upstream] && !self.node_has_buffered_input(upstream),
                    }
                } else {
                    InputFrontier::from_watermark(current_watermark)
                }
            })
            .collect()
    }

    #[cfg(feature = "cluster")]
    fn hold_buffered_source_frontier(
        &self,
        upstream: usize,
        frontier: InputFrontier,
    ) -> InputFrontier {
        if self.node_has_buffered_input(upstream) {
            frontier.held_at(Some(self.output_watermarks[upstream]))
        } else {
            frontier
        }
    }

    pub(super) fn bind_input_tables(&mut self, node: usize, inputs: &[Vec<RecordBatch>]) {
        for (port, batches) in inputs.iter().enumerate() {
            let upstream = self.input_sources[node][port];
            if upstream == usize::MAX {
                continue;
            }
            let name = Arc::clone(&self.nodes[upstream].name);
            if let Some(batch) = batches.first() {
                if !self.live_handles.contains_key(name.as_ref()) {
                    self.ensure_live_provider(&name, &batch.schema());
                }
            }
            if let Some(handle) = self.live_handles.get(name.as_ref()) {
                // Cached SQL plans must see exactly the accepted port input, including batches
                // retained across cycles. Empty ports clear any other branch's provider view.
                handle.swap(batches.clone());
            }
        }
    }

    #[cfg(debug_assertions)]
    pub(super) fn debug_assert_byte_sums(&self) {
        for (id, ports) in self.input_bufs.iter().enumerate() {
            for (port, buf) in ports.iter().enumerate() {
                debug_assert_eq!(
                    self.input_buf_bytes[id][port],
                    retained_input_bytes(buf),
                    "input_buf_bytes drift at node={} port={}",
                    &*self.nodes[id].name,
                    port,
                );
            }
        }
    }
}
