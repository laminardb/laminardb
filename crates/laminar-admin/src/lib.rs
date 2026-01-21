//! # `LaminarDB` Admin Interface

#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic)]

/// REST API - Admin REST API endpoints
pub mod api;

/// Web dashboard - React-based admin dashboard
pub mod dashboard;

/// Command-line tools - CLI administration tools
pub mod cli;