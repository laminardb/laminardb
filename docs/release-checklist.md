# Release Checklist

## Pre-release

- [ ] All CI checks passing on `main`
- [ ] CHANGELOG.md updated with release notes
- [ ] Version bumped in `Cargo.toml` (workspace)
- [ ] All crate versions aligned
- [ ] Documentation updated
- [ ] Breaking changes documented

## Create Release

```bash
# 1. Ensure clean working directory
git status  # Should be clean

# 2. Create and push tag
VERSION="0.3.0"  # Set your version
git tag -a "v${VERSION}" -m "Release v${VERSION}"
git push origin "v${VERSION}"

# 3. Monitor release workflow
# Go to Actions tab and watch the release workflow
```

## Post-release

- [ ] GitHub Release created with all artifacts
- [ ] Checksums file present
- [ ] C header file attached
- [ ] crates.io packages published (check each crate)
- [ ] compatibility.json updated in main branch
- [ ] Documentation deployed to GitHub Pages
- [ ] Announce on relevant channels (if applicable)

## Rollback (if needed)

```bash
# Delete the tag
git tag -d "v${VERSION}"
git push origin --delete "v${VERSION}"

# Delete the GitHub release via web UI
# Yank crates if already published:
cargo yank laminardb --version ${VERSION}
```
