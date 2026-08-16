//! Arrow schema, TLS, and existing collection validation.

use super::{ConnectorError, DataType, Duration, NAMESPACE_EXISTS_CODE};

pub(super) fn is_supported_mongodb_arrow_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Timestamp(..)
    )
}

pub(in crate::mongodb) fn harden_mongodb_tls(
    options: &mut mongodb::options::ClientOptions,
) -> Result<(), ConnectorError> {
    use mongodb::options::{Tls, TlsOptions};

    match options.tls.as_ref() {
        Some(Tls::Enabled(tls)) if tls.allow_invalid_certificates == Some(true) => {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB connection.uri must not set tlsInsecure=true or \
                 tlsAllowInvalidCertificates=true"
                    .into(),
            ));
        }
        Some(Tls::Enabled(_) | Tls::Disabled) => {}
        None => options.tls = Some(Tls::Enabled(TlsOptions::default())),
    }
    Ok(())
}

pub(super) fn is_namespace_exists(error: &mongodb::error::Error) -> bool {
    matches!(
        error.kind.as_ref(),
        mongodb::error::ErrorKind::Command(command) if is_namespace_exists_code(command.code)
    )
}

pub(super) fn is_namespace_exists_code(code: i32) -> bool {
    code == NAMESPACE_EXISTS_CODE
}

pub(super) fn validate_existing_timeseries_spec(
    spec: &mongodb::results::CollectionSpecification,
    expected: &super::super::timeseries::TimeSeriesConfig,
) -> Result<(), ConnectorError> {
    use super::super::timeseries::TimeSeriesGranularity;
    use mongodb::options::TimeseriesGranularity as DriverGranularity;
    use mongodb::results::CollectionType;

    if spec.collection_type != CollectionType::Timeseries {
        return Err(ConnectorError::ConfigurationError(format!(
            "existing MongoDB collection '{}' is not a time series collection",
            spec.name
        )));
    }

    let actual = spec.options.timeseries.as_ref().ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "existing MongoDB time series collection '{}' has no time series options",
            spec.name
        ))
    })?;

    if actual.time_field != expected.time_field {
        return Err(ConnectorError::ConfigurationError(format!(
            "existing MongoDB time series collection '{}' uses time field '{}', expected '{}'",
            spec.name, actual.time_field, expected.time_field
        )));
    }
    if actual.meta_field != expected.meta_field {
        return Err(ConnectorError::ConfigurationError(format!(
            "existing MongoDB time series collection '{}' uses meta field {:?}, expected {:?}",
            spec.name, actual.meta_field, expected.meta_field
        )));
    }

    let granularity_matches = match expected.granularity {
        TimeSeriesGranularity::Seconds => match actual.granularity.as_ref() {
            Some(granularity) => granularity == &DriverGranularity::Seconds,
            None => actual.bucket_max_span.is_none() && actual.bucket_rounding.is_none(),
        },
        TimeSeriesGranularity::Minutes => {
            actual.granularity.as_ref() == Some(&DriverGranularity::Minutes)
        }
        TimeSeriesGranularity::Hours => {
            actual.granularity.as_ref() == Some(&DriverGranularity::Hours)
        }
        TimeSeriesGranularity::Custom {
            bucket_max_span_seconds,
            bucket_rounding_seconds,
        } => {
            actual.granularity.is_none()
                && actual.bucket_max_span
                    == Some(Duration::from_secs(u64::from(bucket_max_span_seconds)))
                && actual.bucket_rounding
                    == Some(Duration::from_secs(u64::from(bucket_rounding_seconds)))
        }
    };
    if !granularity_matches {
        return Err(ConnectorError::ConfigurationError(format!(
            "existing MongoDB time series collection '{}' has incompatible granularity",
            spec.name
        )));
    }

    let expected_ttl = expected.expire_after_seconds.map(Duration::from_secs);
    if spec.options.expire_after_seconds != expected_ttl {
        return Err(ConnectorError::ConfigurationError(format!(
            "existing MongoDB time series collection '{}' uses TTL {:?}, expected {:?}",
            spec.name, spec.options.expire_after_seconds, expected_ttl
        )));
    }

    Ok(())
}
