#!/usr/bin/env bash
# Applies the branch rules in .github/rulesets and turns on GitHub's
# security features. Needs `gh auth login` as the repo owner first.
set -euo pipefail
repo=dartpilot0017/easy_socket

for branch in main dev; do
  name="Protect $branch"
  id=$(gh api "repos/$repo/rulesets" --jq ".[] | select(.name == \"$name\") | .id")
  if [[ -n "$id" ]]; then
    gh api --method PUT "repos/$repo/rulesets/$id" --input ".github/rulesets/$branch.json" >/dev/null
  else
    gh api --method POST "repos/$repo/rulesets" --input ".github/rulesets/$branch.json" >/dev/null
  fi
  echo "Rules applied to $branch."
done

# Label maintainers add after reviewing a change the suspicious check flagged.
gh label create security-reviewed --repo "$repo" --color B60205 \
  --description "A maintainer reviewed the flagged changes" 2>/dev/null || true

# Secret scanning that blocks pushes containing credentials, Dependabot
# alerts and fixes, and private vulnerability reports.
gh api --method PATCH "repos/$repo" --input - >/dev/null <<'JSON'
{"security_and_analysis": {
  "secret_scanning": {"status": "enabled"},
  "secret_scanning_push_protection": {"status": "enabled"}}}
JSON
gh api --method PUT "repos/$repo/vulnerability-alerts" >/dev/null
gh api --method PUT "repos/$repo/automated-security-fixes" >/dev/null
gh api --method PUT "repos/$repo/private-vulnerability-reporting" >/dev/null
echo "Secret scanning, Dependabot alerts and private reporting are on."
