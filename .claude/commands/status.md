# /status

Display current project status.

## Usage

```
/status
```

## Output Sections

### 1. Current Phase
```
## Current Phase: 1 - Core Engine
Progress: 3/12 features complete (25%)
```

### 2. Active Features
```
## Active Work
| Feature | Status | Owner | Blocked? |
|---------|--------|-------|----------|
| F001 | 🚧 In Progress | - | No |
| F003 | 🚧 In Progress | - | Yes (F001) |
```

### 3. Recent Completions
```
## Recently Completed
- F002: Memory-mapped state store (2024-01-15)
```

### 4. Blockers
```
## Blockers
- F003 blocked by F001 (core reactor)
```

### 5. Next Priorities
```
## Next Up
1. F001: Core Reactor Event Loop (P0)
2. F004: Tumbling Windows (P0)
3. F005: DataFusion Integration (P0)
```

### 6. Metrics
```
## Metrics
- Tests: 234 passing, 0 failing
- Coverage: 78%
- Clippy: 0 warnings
- Benchmarks: All targets met
```

## Data Sources

- `docs/features/INDEX.md` - Feature status
- `docs/STEERING.md` - Current priorities
- `cargo test` - Test status
- `cargo clippy` - Lint status

## Example Output

```
╔══════════════════════════════════════════════════════════════╗
║                    LaminarDB Status                          ║
╠══════════════════════════════════════════════════════════════╣
║ Phase: 1 - Core Engine                    Progress: 25%      ║
╠══════════════════════════════════════════════════════════════╣
║ Active:                                                      ║
║   🚧 F001 Core Reactor Event Loop                           ║
║   🚧 F003 State Store Interface                             ║
╠══════════════════════════════════════════════════════════════╣
║ Blockers:                                                    ║
║   ⚠️  F003 waiting on F001                                   ║
╠══════════════════════════════════════════════════════════════╣
║ Next Priority:                                               ║
║   → F004 Tumbling Windows                                   ║
╠══════════════════════════════════════════════════════════════╣
║ Tests: ✅ 234 pass | Coverage: 78% | Clippy: ✅ clean        ║
╚══════════════════════════════════════════════════════════════╝
```
