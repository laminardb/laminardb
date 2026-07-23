# WSL/Docker state-qualification capability check

- **Date:** 2026-07-23
- **Host:** current developer Windows workstation only
- **Evidence class:** local environment capability inventory; not qualification or benchmark evidence
- **Decision:** use for Linux correctness and recovery smoke tests; do not use for gate-bearing storage results

## Result

Docker Desktop is already available on WSL 2 and can run Linux/amd64 containers with cgroup v2.
After provisioning a Rust 1.95 image, this machine is suitable for qualification-tool builds,
schema/golden tests, connector integration, and bounded process-kill/reopen probes. It is not a
substitute for the approved Linux/XFS/dedicated-NVMe qualification host.

The observed storage path is materially different from that target:

```text
container overlayfs
  -> Docker ext4 virtual disk
  -> sparse/dynamic docker_data.vhdx
  -> Windows NTFS C:
  -> shared Windows system NVMe
```

Consequently, results from this host must not be promoted into XFS project-quota, physical-device
write-amplification, `fdatasync`/tail-latency, power-loss, endurance, or production-readiness
evidence. WSL 2 runs Linux inside a managed utility VM, and Docker Desktop stores its WSL engine
data beneath the Windows user profile; that is a useful development environment, not direct device
ownership ([Microsoft WSL architecture](https://learn.microsoft.com/en-us/windows/wsl/about),
[Docker Desktop WSL backend](https://docs.docker.com/desktop/features/wsl/)).

## Observed inventory

The following read-only checks were run from PowerShell, Docker Desktop's already-running Linux
engine, and the local Ubuntu WSL distribution. No package, filesystem, disk, Docker setting, or
service configuration was changed.

| Boundary | Observation |
|---|---|
| WSL | WSL `2.7.10.0`; Microsoft kernel `6.18.33.2`; default Ubuntu distribution uses WSL 2 |
| Docker | Desktop `4.80.0`; client/server `29.6.1`; Linux x86-64 engine; overlayfs; cgroup v2 |
| Docker allocation | 24 CPUs and 16,290,336,768 bytes (about 15.17 GiB); six other containers were running |
| Docker storage | `/var/lib/docker` on an ext4 virtual disk; the backing `docker_data.vhdx` was about 70.25 GiB on NTFS C: |
| Ubuntu storage | Linux root is ext4; this checkout is reached through the Windows/WSL 9p/DrvFs mount, not a native Linux worktree |
| Block identity | Linux exposes Hyper-V virtual SCSI disks, not the host NVMe namespace, controller, firmware, cache, or SMART/NVMe telemetry |
| XFS | Kernel support is present, but no XFS filesystem is mounted and `xfs_quota` is absent; no `prjquota` path exists |
| cgroup | cgroup v2 exposes `memory.stat` dirty/writeback and virtual-device `io.stat`; Docker child cgroups expose `memory.peak` |
| Toolchain | no Rust image is cached; the repository Dockerfile still starts from Rust 1.93 while the workspace requires Rust 1.95 |

The 15.17-GiB Docker memory ceiling and shared virtual disk also cannot satisfy the provisional
64-GiB/96-GiB profile. Microsoft recommends keeping Linux-tool workloads in the Linux filesystem,
not under `/mnt/c`, for both performance and Linux filesystem semantics
([WSL file-storage guidance](https://learn.microsoft.com/en-us/windows/wsl/setup/environment#file-storage)).
Even moving build and test data to a Docker volume would remove the 9p worktree penalty but would
not remove the ext4/VHDX/NTFS/device-virtualization boundary.

## Permitted use and claim boundary

| Activity | This host | Maximum honest claim |
|---|---|---|
| JSON/schema, deterministic codec, golden, model, and compile checks | **YES**, after a pinned Rust 1.95 Linux image is available | Linux build/correctness evidence for the exact image and commit |
| Multi-container source/sink integration | **YES**, subject to resource limits and exact selected-test counts | functional connector/protocol evidence; not delivery certification by itself |
| Process `SIGKILL`, abort, and reopen/recovery | **YES** on a Docker-managed ext4 volume | process-crash atomicity and logical recovery smoke evidence |
| Container/WSL termination as a power-loss test | **NO** | the VM, Windows cache, NTFS, VHDX, and device may still flush; no cache-loss claim |
| cgroup-v2 parser and observer development | **YES** | functional parsing, attribution, and error-path evidence against virtual devices |
| XFS project-ID inheritance and quota syscall | **NO** in the current configuration | requires a separately provisioned XFS `prjquota` mount; XFS defines project quota only on such a filesystem ([kernel XFS documentation](https://docs.kernel.org/6.6/admin-guide/xfs.html#mount-options)) |
| Physical NVMe bytes, flush completion, latency, or endurance | **NO** | virtual `io.stat` is useful diagnostic telemetry only; it cannot identify physical NAND/controller behavior |
| Qualification p95/p99/p999, resource, C3, fault, or endurance gates | **NO** | requires the pinned quiet native Linux/XFS/dedicated-NVMe target |
| Independent production soak | **NO** | a separate operator, immutable release artifact, external oracle, and approved target remain mandatory |

cgroup-v2 remains useful here because its common files define per-cgroup memory and I/O accounting,
including dirty/writeback state, but the values name the virtual block layer visible to the VM rather
than the physical device ([Linux cgroup v2 documentation](https://docs.kernel.org/admin-guide/cgroup-v2.html)).

## Safe local execution lane

The local lane is deliberately narrower than qualification:

1. pin a Rust 1.95 Linux image by digest and record Docker/WSL versions;
2. mount the checkout read-only when possible, and put Cargo target, temporary files, and test
   databases on Docker-managed ext4 volumes rather than the 9p/DrvFs checkout;
3. run deterministic/default/all-feature qualification-tool checks and selected connector tests;
4. run only explicitly labelled process-crash/reopen smoke protocols, with the database on a fresh
   volume per trial;
5. retain `NOT QUALIFICATION EVIDENCE` in every local artifact and reject attempts to merge these
   results with the native-host campaign.

Provisioning a loopback or VHD-backed XFS filesystem could exercise quota plumbing, but would still
sit above VHDX/NTFS and the shared system device. Such a run may be labelled a functional XFS quota
smoke test only; it cannot relax the native target requirement. Raw-attaching the workstation's sole
Windows system NVMe would be invasive and unsafe, so the qualification target must be a separate
device or a separate Linux host.

## Reproduction commands

The inventory was derived from these non-destructive queries (outputs are environment snapshots,
not checked-in benchmark data):

```powershell
wsl --version
wsl --status
wsl --list --verbose
docker version
docker context show
docker info
docker image ls
Get-PhysicalDisk
Get-Volume -DriveLetter C
```

Linux-side inspection used `uname -a`, `findmnt`, `df -T`, `lsblk`, `/proc/mounts`,
`/proc/self/cgroup`, `/sys/fs/cgroup/{cgroup.controllers,memory.stat,io.stat}`, and checks for an XFS
mount and `xfs_quota`. Any later capability claim must rerun and retain the exact inventory because
Docker allocation, active workloads, WSL kernel, and storage placement are mutable local state.
