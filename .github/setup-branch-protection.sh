#!/usr/bin/env bash
# Setup branch protection rules for the main branch.
# Requires: gh CLI authenticated with admin access to the repo.
#
# Usage: bash .github/setup-branch-protection.sh [OWNER/REPO]
# Example: bash .github/setup-branch-protection.sh laminardb/laminardb

set -euo pipefail

REPO="${1:-$(gh repo view --json nameWithOwner -q .nameWithOwner)}"

echo "Configuring branch protection for ${REPO} (main branch)..."

gh api --method PUT "repos/${REPO}/branches/main/protection" \
  --input - <<'EOF'
{
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "CI Success",
      "Human Review Attestation"
    ]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "required_approving_review_count": 1,
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": true
  },
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false
}
EOF

echo "Branch protection configured successfully for ${REPO}:main"
echo ""
echo "Rules applied:"
echo "  - Required checks: CI Success, Human Review Attestation"
echo "  - Must be up to date before merging"
echo "  - 1 approving review required (from CODEOWNERS)"
echo "  - Stale reviews dismissed on new pushes"
echo "  - Admins cannot bypass rules"
echo "  - Force pushes and branch deletion blocked"
