//! # `LaminarDB` Observability

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// Metrics collection and export - Prometheus and OpenTelemetry metrics
pub mod metrics;

/// Distributed tracing - OpenTelemetry tracing integration
pub mod tracing;

/// Health check endpoints - Liveness and readiness probes
pub mod health;
