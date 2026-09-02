# Cloud object-store support

**Status:** current implementation boundary, 2026-09-02

Cloud storage is a backend capability, not a file or table format. This page records support for
each storage consumer separately. It also separates code/configuration support from evidence: a
build, unit test, or emulator run is not native-provider certification.

## Status terms

- **Compiled**: the provider-specific minimal Cargo feature is built in CI.
- **URI/config supported**: startup validates and adapts the provider location and configuration.
- **Emulator smoke-tested**: an emulator or compatible endpoint exercises I/O; this is never native
  evidence.
- **Native integration-tested**: all required native integration tests ran against the exact commit
  and produced an eligible JSON artifact.
- **Native fault-soak certified**: the protected native fault workflow passed without skips against
  the exact commit and uploaded eligible evidence.
- **Exact-delivery admitted**: production admission permits the composition. Admission is an
  independent fail-closed policy, not a consequence of compilation or a single integration test.
- **Experimental**: usable behind an explicit feature, but either its upstream implementation or
  LaminarDB evidence is not yet sufficient for general-availability claims.
- **Unsupported**: rejected at configuration/startup before backend I/O.

No native AWS, Azure, or GCS evidence artifact was produced while preparing this change. The
protected workflow exists for future runs, but an unexecuted or skipped job does not certify its
provider.

## Capability matrix

“Native job” means a protected job is implemented but has not established certification for the
current commit. “ALO” means at-least-once only in cluster mode.

| Consumer | AWS S3 | Azure Blob / ADLS Gen2 | Google Cloud Storage | Local filesystem | S3-compatible endpoint |
|---|---|---|---|---|---|
| Checkpoint/recovery | URI/config; native capability + fault jobs; no current artifact | URI/config; Azurite Blob conditional/CAS/fault smoke job; native capability + fault jobs; no current native artifact | URI/config; GCS-emulator basic I/O/restart smoke only; no emulator multipart/CAS/fault evidence; keyless native job enabled; no current native artifact | Supported through the built-in local directory; only absolute `file://` URLs classify as node-durable | URI/config; MinIO smoke coverage; compatibility tier only |
| Delta source | URI/config; MinIO smoke; native job; local/embedded source semantics | URI/config; Azurite read/reopen smoke job; native job; no current native artifact | URI/config; GCS-emulator read/reopen smoke job; keyless native job enabled; no current native artifact | Supported | MinIO smoke; compatibility tier only |
| Delta sink | URI/config; MinIO smoke; native job; direct S3/S3A cluster exact admission retained | URI/config; coordinated-publication emulator smoke job; native job; cluster ALO only | URI/config; emulator append/read/reopen smoke at ALO only; keyless native job enabled; no current native artifact; cluster ALO only | Supported; local coordinated delivery rules apply | MinIO smoke; cluster ALO only |
| Iceberg source | REST + S3/file wiring; MinIO smoke; native job | **Experimental** `iceberg-azure`; native job; no current artifact | `iceberg-gcs`; native job; no current artifact | Supported by `iceberg` | MinIO smoke; compatibility tier only |
| Iceberg sink | REST + S3/file wiring; existing direct-S3 admission retained; native job | **Experimental** `iceberg-azure`; ALO only; no current artifact | `iceberg-gcs`; ALO only; no current artifact | Supported; local coordinated delivery rules apply | MinIO smoke; no native certification claim |
| Files source (Parquet/CSV/JSON) | Unsupported remote URL | Unsupported remote URL | Unsupported remote URL | Supported | Unsupported remote URL |
| Files sink (Parquet/CSV/JSON) | Unsupported remote URL | Unsupported remote URL | Unsupported remote URL | Supported | Unsupported remote URL |

The Files connector is deliberately local-only. Remote URLs are detected before any `std::fs`
operation and return an actionable error. No object-store sink was added because a safe remote sink
needs deterministic keys, conditional publication, content validation on retry, and a durable
visibility manifest; POSIX rename semantics are not emulated over object storage.

Cluster exact-delivery admission is unchanged. It remains limited to the compositions documented
by the runtime admission checks: Kafka input with the certified direct S3/S3A append-mode Delta
sink, or the certified REST-catalog Iceberg append sink backed by direct S3/S3A storage. Custom S3
endpoints, Azure, and GCS do not inherit that admission.

## Locations and canonicalisation

All consumers use one parsed provider/location model, then a small consumer-specific adapter.
Schemes are case-insensitive. Query parameters, fragments, and credential-bearing user-info are
rejected; credentials, SAS tokens, and signed query parameters must be configured separately. The
user-info component in a qualified Azure Hadoop URL carries its filesystem/container rather than a
credential. `http://`, `https://`, and `s3n://` are not storage locations.

| Provider | Accepted input | Consumer form |
|---|---|---|
| AWS | `s3://bucket/prefix`, `s3a://bucket/prefix` | S3/S3A is retained for object_store, Delta, and Iceberg |
| GCS | `gs://bucket/prefix`, `gcs://bucket/prefix` | `gcs://` is canonicalised to `gs://` |
| Azure object_store/Delta | `az://container/prefix`; qualified `abfs[s]://` and `wasb[s]://` | Qualified authorities become `az://container/prefix` plus derived account/container/endpoint options |
| Azure Iceberg/OpenDAL | `abfs[s]://filesystem@account.dfs.<suffix>/prefix`; `wasb[s]://container@account.blob.<suffix>/prefix` | The complete qualified URL is retained, including sovereign/private endpoint suffixes |
| Local | absolute `file://` URL | Absolute local path; a remote authority or relative file location is rejected |

An unqualified Azure shorthand can remain valid for object_store/Delta when the account is supplied
by configuration. Iceberg requires the fully qualified form because the pinned OpenDAL AzDLS
backend resolves the filesystem, account, service, and endpoint suffix from each object URL.
Repeated slashes, dot segments, percent escapes, spaces, and Unicode in object prefixes are not
normalised away.

Azure Iceberg remains experimental for both ADLS Gen2 and Blob-style aliases. In particular, the
pinned AzDLS implementation accepts `wasb[s]://` input but constructs its service endpoint through
the ADLS/DFS path internally; native Blob-endpoint behavior has not been established. Use of a
Blob-style alias is therefore configuration wiring, not evidence of general Blob interoperability.

Known public and sovereign Azure cloud suffixes are classified as native. Other qualified suffixes
are still preserved for private/compatibility deployments, but are classified as custom and cannot
produce native-certification evidence.

## Features

| Consumer | Minimal feature |
|---|---|
| Checkpoint core | `laminar-core/aws`, `laminar-core/azure`, or `laminar-core/gcs` |
| Checkpoint through DB/server | `checkpoint-aws`, `checkpoint-azure`, or `checkpoint-gcs` |
| Delta | `delta-lake-s3`, `delta-lake-azure`, or `delta-lake-gcs` |
| Iceberg REST + S3 + local | `iceberg` |
| Iceberg REST + GCS | `iceberg-gcs` |
| Iceberg REST + Azure | `iceberg-azure` (experimental) |
| Local Files | `files` |

The existing `aws`, `azure`, and `gcs` DB/server features remain aliases for checkpoint-provider
wiring for backward compatibility. They do not imply that Delta, Iceberg, or Files is enabled.
The lower-level `iceberg-storage-*` features remain available in `laminar-connectors`; applications
should normally use the provider-specific public features above. The pinned
`iceberg-storage-opendal` 0.10.1 provider features are used directly; the broad OpenDAL service set
is not enabled.

## Credentials and endpoints

Resolution order is explicit connector/server configuration, explicitly selected profile or
credential source, provider environment/configuration, then the downstream default chain. Empty
values are ignored. LaminarDB classifies the selected mechanism for non-secret diagnostics but does
not cache refreshable tokens or replace SDK/provider default chains with static credentials.

- AWS options include `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`,
  `aws_web_identity_token_file`, `aws_role_arn`, region, endpoint, addressing style, and
  `aws_allow_http`. Web identity, container credentials, and instance metadata refresh downstream.
  `aws_profile`/`AWS_PROFILE` is supported by the Delta AWS integration, but the pinned
  `object_store` checkpoint client has no shared-config/profile loader; use web identity, container
  credentials, instance metadata, or explicit credentials for checkpoint storage. HTTP is valid
  only for an explicitly configured compatibility endpoint.
- Azure options include account name/key, SAS, tenant/client/client-secret, federated token file,
  authority host, managed-identity endpoint, and Azure CLI selection. Checkpoint and Delta use the
  pinned `object_store` workload-identity, managed-identity, or Azure CLI providers. The
  experimental Iceberg AzDLS adapter accepts account/key, SAS, service-principal properties, and
  its own Azure CLI/managed-identity default chain; it does not currently map a federated-token
  file property. A qualified Azure URL and conflicting explicit account/endpoint configuration
  fail closed.
- GCS checkpoint and Delta accept service-account JSON/path, Application Default Credentials,
  metadata discovery, and `external_account` workload-identity federation files. The pinned
  `object_store` 0.13.2 data client is retained; for external accounts LaminarDB injects the
  official `google-cloud-auth` refreshable provider through its credential-provider hook. The
  provider, rather than LaminarDB, owns token caching and refresh. Iceberg/OpenDAL independently
  supports `gcs.credentials-json`, `gcs.oauth2.token`, service-account paths, external-account
  ADC, and metadata/default discovery. Custom endpoints and no-auth are compatibility/test modes,
  not native evidence.
- Iceberg passes exact upstream properties through `storage.property.*`. Common keys are
  `s3.*`, `gcs.credentials-json`, `gcs.oauth2.token`, `gcs.service.path`, and `adls.account-name`,
  `adls.account-key`, `adls.sas-token`, `adls.tenant-id`, `adls.client-id`,
  `adls.client-secret`, and `adls.authority-host`. `storage.endpoint` maps to S3/GCS only;
  the pinned experimental AzDLS adapter derives its endpoint from the fully qualified URL.

The code-side GCS WIF gate is closed, so protected checkpoint and Delta jobs include GCS on manual
and scheduled matrices. This is not a GCS certification or delivery-admission claim: promotion
still requires protected environment setup and eligible exact-commit native capability and fault
artifacts. The GCS Iceberg job continues to use OpenDAL's independent credential implementation.

Endpoint overrides are classified in diagnostics without hostname, path, query, bucket, account,
container, or object key. An S3 override is **S3-compatible**, while Azure/GCS overrides are
**custom/emulator**. Debug/error/evidence paths expose option names and an authentication category,
never option values or full signed URLs.

## Emulator smoke workflow

`.github/workflows/cloud-object-store-emulator-smoke.yml` runs on pull requests without cloud
credentials. It uses version-pinned Azurite and GCS-compatible containers, creates a per-job test
container/bucket, and emits artifacts that are explicitly classified as emulator evidence.

The Azure job executes the shared conditional-create, stale-CAS, fresh-client, cleanup, and
checkpoint fault-boundary contracts. It also exercises Delta's coordinated descriptor publication,
conflicting-cut rejection, idempotent retry, read, and fresh-client cursor recovery. Coordinated
emulator endpoints are admitted only behind explicit debug/soak markers; release builds remain
fail-closed.

The GCS job is deliberately narrower. The fake server used by pinned `object_store 0.13.2` does not
implement XML multipart upload or honor `ifGenerationMatch`, so it runs basic put/get/range/head,
list/delete, prefix-isolation, cleanup, fresh-client, and Delta ALO append/read/reopen smoke only.
Its evidence records conditional-create, stale-CAS, and multipart as unproven (`false`).

Azurite covers the Azure Blob protocol only and does not emulate ADLS Gen2 hierarchical namespace
behavior. The GCS-compatible container does not establish native generation, identity, retry, or
service behavior. Artifacts from this workflow therefore say `native_or_emulator=emulator` and are
never eligible to promote delivery admission.

## Native evidence workflow

`.github/workflows/native-cloud-object-store-soak.yml` runs only on a protected schedule or manual
dispatch. It uses OIDC/workload identity and unique prefixes of the form
`laminardb-tests/<sha>/<run-id>/<suite>/<uuid>/`. Each provider job rejects endpoint overrides,
requires a native marker, treats missing setup as failure, rejects exact-delivery selection until a
matching provider fault contract exists, cleans up in an always path, validates the tested SHA and
zero-skip result, and uploads JSON evidence.

Protected GitHub environments named `native-cloud-aws`, `native-cloud-azure`, and
`native-cloud-gcs` supply only identifiers and locations:

- AWS: OIDC-trusted role, region, and `LAMINAR_AWS_TEST_URL`; grant list/read/create/update/delete
  only for the pre-provisioned test bucket/prefix.
- Azure: federated client, tenant and subscription identifiers plus `LAMINAR_AZURE_TEST_URL`; grant
  data-plane list/read/create/update/delete only for the test container. Iceberg additionally uses
  `LAMINAR_AZURE_ICEBERG_TEST_URL` in the fully qualified `abfs[s]://` or `wasb[s]://` form because
  its OpenDAL adapter derives account, filesystem/container, and service from the URL.
- GCP: Workload Identity Provider, service account, project, and `LAMINAR_GCS_TEST_URL`; grant
  object list/read/create/update/delete only for the test bucket/prefix.
- Iceberg jobs additionally require the provider-specific
  `LAMINAR_<PROVIDER>_ICEBERG_CATALOG_URI` and a pre-provisioned REST catalog/namespace policy.

Actual account IDs, project IDs, tenant IDs, bucket names, secrets, and signed URLs must never be
committed. Emulator runs belong in pull-request smoke coverage and must report
`native_or_emulator=emulator`.

Promotion requires every required native capability and fault test to run without skips against
the exact production commit, pass atomic create/stale-CAS/restart/cleanup assertions, and upload a
validated artifact. A later or different commit does not automatically inherit the result, and an
evidence job does not automatically change delivery admission.
