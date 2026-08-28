use std::cell::RefCell;
use std::future::Future;

use crate::error::ConnectorError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IcebergFaultPoint {
    BeforeFileClose,
    AfterFileClose,
    AfterDescriptor,
    BeforeCatalogCommit,
    AfterCatalogCommit,
    DuringMetadataRefresh,
    DuringManifestReconciliation,
    DuringCommittedCursor,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct IcebergFault {
    point: IcebergFaultPoint,
    occurrence: usize,
}

impl IcebergFault {
    pub(super) const fn first(point: IcebergFaultPoint) -> Self {
        Self {
            point,
            occurrence: 1,
        }
    }

    pub(super) const fn on_occurrence(point: IcebergFaultPoint, occurrence: usize) -> Self {
        assert!(occurrence > 0, "fault occurrence must be nonzero");
        Self { point, occurrence }
    }
}

#[derive(Debug)]
struct ArmedFault {
    point: IcebergFaultPoint,
    remaining: usize,
}

tokio::task_local! {
    static ACTIVE_FAULTS: RefCell<Vec<ArmedFault>>;
}

pub(super) async fn scope<I, F>(faults: I, future: F) -> F::Output
where
    I: IntoIterator<Item = IcebergFault>,
    F: Future,
{
    let faults = faults
        .into_iter()
        .map(|fault| ArmedFault {
            point: fault.point,
            remaining: fault.occurrence,
        })
        .collect();
    ACTIVE_FAULTS.scope(RefCell::new(faults), future).await
}

pub(super) fn hit(point: IcebergFaultPoint) -> bool {
    ACTIVE_FAULTS
        .try_with(|faults| {
            let mut faults = faults.borrow_mut();
            let Some(index) = faults.iter().position(|fault| fault.point == point) else {
                return false;
            };
            let fault = &mut faults[index];
            fault.remaining -= 1;
            if fault.remaining != 0 {
                return false;
            }
            faults.remove(index);
            true
        })
        .unwrap_or(false)
}

pub(super) fn fail_if(point: IcebergFaultPoint) -> Result<(), ConnectorError> {
    if hit(point) {
        return Err(ConnectorError::Internal(format!(
            "[LDB-ICEBERG-FAULT-INJECTION] {point:?}"
        )));
    }
    Ok(())
}

pub(super) fn fail_outcome_unknown_if(point: IcebergFaultPoint) -> Result<(), ConnectorError> {
    if hit(point) {
        return Err(ConnectorError::outcome_unknown(
            format!("[LDB-ICEBERG-FAULT-INJECTION] {point:?}"),
            true,
        ));
    }
    Ok(())
}
