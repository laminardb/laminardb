//! Batch admission, cursor fencing, and decoded output assembly.

use super::{
    debug, kafka_row_positions, lock_or_recover, normalize_kafka_debezium_batch,
    retire_accepted_rotation_baselines, rotation_baselines_len, rotation_partition_baseline,
    terminalize_guaranteed_poll_error, vnode_payload_is_current, Arc, ConnectorError,
    ConnectorState, Format, KafkaAssignmentPublication, KafkaPayload, KafkaReaderItem, KafkaSource,
    Ordering, SourceBatch,
};

struct DrainedQueue {
    assignment: Option<Arc<KafkaAssignmentPublication>>,
    retires_rotation_baseline: bool,
}

enum QueueItemDecision {
    Accept(KafkaPayload),
    Skip,
    Boundary,
}

impl KafkaSource {
    pub(super) async fn poll_batch_inner(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        let limit = self.prepare_poll(max_records)?;

        let include_metadata = self.config.include_metadata;
        let include_headers = self.config.include_headers;

        let Some(drained) = self.drain_reader_queue(limit)? else {
            return Ok(None);
        };
        // Decode can await, so retain the immutable assignment cut by value and release its locks.
        let total_bytes = self.stage_poll_payloads(include_metadata, include_headers);
        self.reconcile_reader_control_state();

        if self.poll_payload_offsets.is_empty() {
            return Ok(None);
        }

        let output = self
            .decode_poll_output(include_metadata, include_headers)
            .await?;
        let num_rows = output.num_rows();

        self.publish_staged_offsets();
        let output = self.attach_batch_checkpoint(
            output,
            drained.assignment,
            drained.retires_rotation_baseline,
        )?;
        self.poll_staged_offsets.clear();

        self.metrics.record_poll(num_rows as u64, total_bytes);
        debug!(
            records = num_rows,
            bytes = total_bytes,
            "polled batch from Kafka"
        );
        Ok(Some(output))
    }

    fn drain_reader_queue(&mut self, limit: usize) -> Result<Option<DrainedQueue>, ConnectorError> {
        // Pin one ownership publication while draining the non-awaiting queue so a cut cannot mix
        // ownership versions. Assignment writers wait only for this short critical section.
        let drained = {
            let vnode_registry = self
                .vnode_assignment
                .as_ref()
                .map(|(registry, self_id)| (Arc::clone(registry), *self_id));
            let vnode_publication = vnode_registry
                .as_ref()
                .map(|(registry, self_id)| (registry.read_assignment(), *self_id));
            if let Some((published, _)) = vnode_publication.as_ref() {
                if self.reconciled_assignment_version.load(Ordering::Acquire) != published.version()
                {
                    return Ok(None);
                }
            }

            let assignment = match vnode_publication.as_ref() {
                Some((published, _)) => {
                    let assignment = Arc::clone(&lock_or_recover(&self.assignment_publication));
                    if assignment.assignment_version != published.version() {
                        return Ok(None);
                    }
                    Some(assignment)
                }
                None => None,
            };

            let rotation_baselines = assignment
                .as_deref()
                .filter(|publication| !publication.baselines.is_empty());
            self.apply_rotation_baseline_fence(rotation_baselines);
            let vnode_ownership = vnode_publication
                .as_ref()
                .map(|(published, self_id)| (published.owners(), *self_id));

            while self.poll_payloads.len() < limit {
                let Some(item) = self.try_take_reader_item()? else {
                    break;
                };
                let ownership = vnode_ownership
                    .as_ref()
                    .map(|(owners, self_id)| (*owners, *self_id));
                match self.classify_reader_item(item, ownership, rotation_baselines)? {
                    QueueItemDecision::Accept(payload) => self.poll_payloads.push(payload),
                    QueueItemDecision::Skip => {}
                    QueueItemDecision::Boundary => break,
                }
            }
            let retires_baseline = rotation_baselines.is_some_and(|publication| {
                self.poll_payloads.iter().any(|payload| {
                    rotation_partition_baseline(
                        &publication.baselines,
                        payload.topic.as_ref(),
                        payload.partition,
                    )
                    .is_some_and(|next| payload.offset >= next)
                })
            });
            DrainedQueue {
                assignment,
                retires_rotation_baseline: retires_baseline,
            }
        };
        Ok(Some(drained))
    }

    fn apply_rotation_baseline_fence(&mut self, publication: Option<&KafkaAssignmentPublication>) {
        let Some(publication) = publication else {
            return;
        };
        if self.applied_rotation_baseline_version == Some(publication.assignment_version) {
            return;
        }
        let mut snapshot = lock_or_recover(&self.offset_snapshot);
        for (topic, partitions) in &publication.baselines {
            for partition in partitions.keys() {
                self.offsets.remove(topic, *partition);
                snapshot.remove(topic, *partition);
            }
        }
        self.applied_rotation_baseline_version = Some(publication.assignment_version);
    }

    fn try_take_reader_item(&mut self) -> Result<Option<KafkaReaderItem>, ConnectorError> {
        let rx = self
            .msg_rx
            .as_mut()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "reader initialized".into(),
                actual: "reader is None".into(),
            })?;
        match rx.try_recv() {
            Ok(item) => {
                self.channel_len.fetch_sub(1, Ordering::Release);
                Ok(Some(item))
            }
            Err(crossfire::TryRecvError::Empty) => Ok(None),
            Err(crossfire::TryRecvError::Disconnected) => {
                self.state = ConnectorState::Failed;
                Err(ConnectorError::Internal(
                    "Kafka reader task exited unexpectedly".into(),
                ))
            }
        }
    }

    fn classify_reader_item(
        &mut self,
        item: KafkaReaderItem,
        ownership: Option<(&[laminar_core::state::NodeId], laminar_core::state::NodeId)>,
        rotation: Option<&KafkaAssignmentPublication>,
    ) -> Result<QueueItemDecision, ConnectorError> {
        let KafkaReaderItem::Payload(payload) = item else {
            let KafkaReaderItem::DrainBoundary(boundary) = item else {
                unreachable!();
            };
            let Some(active) = self.source_drain.as_mut() else {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::Internal(
                    "Kafka reader emitted a drain boundary without an active round".into(),
                ));
            };
            if boundary.round != active.request.round || active.boundary.is_some() {
                self.state = ConnectorState::Failed;
                return Err(ConnectorError::Internal(
                    "Kafka reader emitted a stale or duplicate drain boundary".into(),
                ));
            }
            active.boundary = Some(boundary);
            return Ok(QueueItemDecision::Boundary);
        };

        let required_next = rotation.and_then(|publication| {
            rotation_partition_baseline(
                &publication.baselines,
                payload.topic.as_ref(),
                payload.partition,
            )
        });
        let is_current = vnode_payload_is_current(
            ownership,
            payload.partition_vnode,
            required_next,
            payload.offset,
        )
        .map_err(|error| {
            terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                error,
            )
        })?;
        if is_current {
            return Ok(QueueItemDecision::Accept(payload));
        }
        debug!(
            topic = payload.topic.as_ref(),
            partition = payload.partition,
            offset = payload.offset,
            "discarded Kafka payload outside the current vnode handoff cut"
        );
        Ok(QueueItemDecision::Skip)
    }

    fn prepare_poll(&mut self, max_records: usize) -> Result<usize, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "Running".into(),
                actual: self.state.to_string(),
            });
        }
        self.ensure_reader_started();
        self.check_reader_health("polling source data")?;

        // Preserve allocations across polls and discard cursor state left by failed finalization.
        self.poll_payloads.clear();
        self.poll_payload_buf.clear();
        self.poll_payload_offsets.clear();
        self.poll_staged_offsets.clear();
        self.poll_meta_partitions.clear();
        self.poll_meta_offsets.clear();
        self.poll_meta_timestamps.clear();
        self.poll_meta_headers.clear();
        Ok(max_records.min(self.config.max_poll_records))
    }

    async fn decode_poll_output(
        &mut self,
        include_metadata: bool,
        include_headers: bool,
    ) -> Result<SourceBatch, ConnectorError> {
        let (batch, good_indices) = self.decode_polled_payloads().await?;
        let (batch, mutations) = if self.config.format == Format::Debezium {
            normalize_kafka_debezium_batch(&batch, &self.schema).map_err(|error| {
                terminalize_guaranteed_poll_error(
                    self.delivery,
                    &mut self.state,
                    &self.metrics,
                    self.reader_shutdown.as_ref(),
                    error,
                )
            })?
        } else {
            (batch, None)
        };
        let row_positions = kafka_row_positions(
            self.source_name.as_ref(),
            &self.poll_staged_offsets,
            good_indices.as_deref(),
        )
        .map_err(|error| {
            terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                error,
            )
        })?;

        let batch = self.append_metadata_columns(
            batch,
            good_indices.as_deref(),
            include_metadata,
            include_headers,
        )?;
        // Construct the complete output before publishing its cursor. In particular,
        // metadata/header column validation above is fallible and must not retire a rotation
        // baseline or advance the recovery position for a batch that cannot be returned.
        let output = SourceBatch::positioned(batch, row_positions).map_err(|error| {
            terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                error,
            )
        })?;
        if let Some(mutations) = mutations {
            output.with_mutations(mutations).map_err(|error| {
                terminalize_guaranteed_poll_error(
                    self.delivery,
                    &mut self.state,
                    &self.metrics,
                    self.reader_shutdown.as_ref(),
                    error,
                )
            })
        } else {
            Ok(output)
        }
    }

    fn stage_poll_payloads(&mut self, include_metadata: bool, include_headers: bool) -> u64 {
        let mut total_bytes = 0;
        for payload in self.poll_payloads.drain(..) {
            total_bytes += payload.data.len() as u64;
            let start = self.poll_payload_buf.len();
            self.poll_payload_buf.extend_from_slice(&payload.data);
            self.poll_payload_offsets.push((start, payload.data.len()));
            self.poll_staged_offsets.push((
                Arc::clone(&payload.topic),
                payload.partition,
                payload.offset,
            ));

            if include_metadata {
                self.poll_meta_partitions.push(payload.partition);
                self.poll_meta_offsets.push(payload.offset);
                self.poll_meta_timestamps.push(payload.timestamp_ms);
            }
            if include_headers {
                self.poll_meta_headers.push(payload.headers_json);
            }
        }
        total_bytes
    }

    fn reconcile_reader_control_state(&mut self) {
        let rebalance_events = self.rebalance_counter.swap(0, Ordering::Relaxed);
        for _ in 0..rebalance_events {
            self.metrics.record_rebalance();
        }

        let current_revoke_gen = self.revoke_generation.load(Ordering::Acquire);
        if current_revoke_gen == self.last_seen_revoke_gen {
            return;
        }
        self.last_seen_revoke_gen = current_revoke_gen;
        let assigned = lock_or_recover(&self.rebalance_state).assignment_snapshot();
        let before = self.offsets.partition_count();
        self.offsets.retain_assigned(&assigned);
        lock_or_recover(&self.offset_snapshot).retain_assigned(&assigned);
        let after = self.offsets.partition_count();
        if before != after {
            debug!(
                before,
                after, "purged revoked partition offsets after rebalance"
            );
        }
    }

    fn publish_staged_offsets(&mut self) {
        if self.poll_staged_offsets.is_empty() {
            return;
        }
        let mut snapshot = lock_or_recover(&self.offset_snapshot);
        for (topic, partition, offset) in &self.poll_staged_offsets {
            self.offsets.update_arc(topic, *partition, *offset);
            snapshot.update_arc(topic, *partition, *offset);
        }
    }

    fn attach_batch_checkpoint(
        &mut self,
        output: SourceBatch,
        drained_assignment: Option<Arc<KafkaAssignmentPublication>>,
        retires_rotation_baseline: bool,
    ) -> Result<SourceBatch, ConnectorError> {
        match drained_assignment {
            Some(assignment)
                if self.batch_cursor_assignment_version == Some(assignment.assignment_version) =>
            {
                let assignment_version = assignment.assignment_version;
                match self.capture_vnode_checkpoint_delta(&assignment) {
                    Err(error) => Err(error),
                    Ok(delta) => {
                        drop(assignment);
                        if retires_rotation_baseline {
                            let mut published = lock_or_recover(&self.assignment_publication);
                            if published.assignment_version == assignment_version {
                                let publication = Arc::make_mut(&mut published);
                                retire_accepted_rotation_baselines(
                                    &mut publication.baselines,
                                    &self.poll_staged_offsets,
                                );
                                let count = rotation_baselines_len(&publication.baselines);
                                self.rotation_partition_baseline_count
                                    .store(count, Ordering::Release);
                            }
                        }
                        Ok(output.with_checkpoint_delta(delta))
                    }
                }
            }
            Some(assignment) => {
                let accepted = if retires_rotation_baseline {
                    let mut accepted = (*assignment).clone();
                    retire_accepted_rotation_baselines(
                        &mut accepted.baselines,
                        &self.poll_staged_offsets,
                    );
                    Arc::new(accepted)
                } else {
                    assignment
                };
                if retires_rotation_baseline {
                    let mut published = lock_or_recover(&self.assignment_publication);
                    if published.assignment_version == accepted.assignment_version {
                        *published = Arc::clone(&accepted);
                        let count = rotation_baselines_len(&accepted.baselines);
                        self.rotation_partition_baseline_count
                            .store(count, Ordering::Release);
                    }
                }
                self.capture_vnode_checkpoint(&accepted).map(|checkpoint| {
                    self.batch_cursor_assignment_version = Some(accepted.assignment_version);
                    output.with_checkpoint(checkpoint)
                })
            }
            None => Ok(output),
        }
        .map_err(|error| {
            terminalize_guaranteed_poll_error(
                self.delivery,
                &mut self.state,
                &self.metrics,
                self.reader_shutdown.as_ref(),
                error,
            )
        })
    }
}
