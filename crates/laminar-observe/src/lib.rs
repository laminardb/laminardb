//! # LaminarDB Observability

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Metrics collection and export
pub mod metrics {
    //! Prometheus and OpenTelemetry metrics
}

/// Distributed tracing
pub mod tracing {
    //! OpenTelemetry tracing integration
}

/// Health check endpoints
pub mod health {
    //! Liveness and readiness probes
}