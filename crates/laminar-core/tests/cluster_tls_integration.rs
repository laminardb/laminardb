#![cfg(feature = "cluster")]

use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use laminar_core::cluster::control::barrier::BARRIER_ADDR_KEY;
use laminar_core::cluster::control::{
    remote_scan_client, set_cluster_tls, BarrierCoordinator, ClusterKv, ClusterTls, InMemoryKv,
    QueryClientPool, QueryHandlerSlot, RemoteQueryHandler,
};
use laminar_core::cluster::discovery::NodeId;

struct StaticHandler(RecordBatch);

#[async_trait::async_trait]
impl RemoteQueryHandler for StaticHandler {
    async fn remote_scan(
        &self,
        _table_name: &str,
        projection: Option<Vec<usize>>,
        _filter_sql: Option<String>,
    ) -> Result<RecordBatch, String> {
        match projection {
            Some(projection) => self
                .0
                .project(&projection)
                .map_err(|error| error.to_string()),
            None => Ok(self.0.clone()),
        }
    }
}

#[tokio::test]
async fn remote_scan_uses_mutual_tls() {
    const SAN: &str = "laminar-cluster";

    let mut ca_params = rcgen::CertificateParams::new(vec!["laminar-test-ca".into()]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let mut leaf = rcgen::CertificateParams::new(vec![SAN.into()]).unwrap();
    leaf.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_cert = leaf.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();
    set_cluster_tls(ClusterTls::from_pem(
        leaf_cert.pem().as_bytes(),
        leaf_key.serialize_pem().as_bytes(),
        ca_cert.pem().as_bytes(),
        SAN,
    ))
    .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
    let peer = NodeId(7);
    let remote = BarrierCoordinator::new(Arc::new(InMemoryKv::new(peer)));
    let handler: Arc<dyn RemoteQueryHandler> = Arc::new(StaticHandler(batch));
    let slot: QueryHandlerSlot = Arc::new(parking_lot::RwLock::new(Some(handler)));
    let remote_addr = remote
        .start_server("127.0.0.1:0".parse().unwrap(), None, slot)
        .await
        .unwrap();

    let caller = Arc::new(InMemoryKv::new(NodeId(1)));
    caller.seed(peer, BARRIER_ADDR_KEY, remote_addr.to_string());
    let kv: Arc<dyn ClusterKv> = caller;
    let pool: QueryClientPool = Arc::new(parking_lot::Mutex::new(Default::default()));
    let stream = remote_scan_client(&pool, &kv, peer, "mv", None, None)
        .await
        .unwrap()
        .expect("peer is resolvable");
    let batches = stream
        .map(|batch| batch.expect("mTLS remote scan batch"))
        .collect::<Vec<_>>()
        .await;
    let combined = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let values = combined
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(values.values(), &[1, 2, 3]);
}
