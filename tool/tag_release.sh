#!/usr/bin/env bash
# Tags the version in pubspec.yaml (v1.2.3) if that tag doesn't exist yet.
# The tag goes on the commit that set the version, so a late run still tags
# the right release. Runs on every push to main; does nothing otherwise.
#
# DRY_RUN=1 tool/tag_release.sh  shows what it would do without pushing.
set -euo pipefail

version=$(sed -n 's/^version:[[:space:]]*//p' pubspec.yaml | tr -d '[:space:]')
if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]]; then
  echo "No valid version in pubspec.yaml (found '$version')." >&2
  exit 1
fi
tag="v$version"

if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
  echo "$tag already exists. Nothing to do."
  exit 0
fi

# The most recent commit whose pubspec.yaml change touched this version line.
escaped=${version//./\\.}
commit=$(git log -E -G"^version:[[:space:]]*${escaped}[[:space:]]*$" \
  --format=%H -1 -- pubspec.yaml)
commit=${commit:-$(git rev-parse HEAD)}

git -c user.name="github-actions[bot]" \
    -c user.email="41898282+github-actions[bot]@users.noreply.github.com" \
    tag -a "$tag" "$commit" -m "easy_socket $version"

if [[ "${DRY_RUN:-}" == 1 ]]; then
  echo "Would push $tag at $(git log -1 --format='%h %s' "$commit")"
  exit 0
fi
git push origin "refs/tags/$tag"
echo "Created $tag at $(git log -1 --format='%h %s' "$commit")"
