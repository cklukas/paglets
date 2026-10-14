#!/bin/sh
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
#
# Writes the tap's formula for a published release: packaging/homebrew/
# paglets.rb with url and sha256 of that release's source archive.
#
#   update-formula.sh VERSION SHA256 OWNER/REPOSITORY OUT.rb
#
# The release workflow (.github/workflows/cpp-release.yml) runs it and pushes
# OUT.rb to the tap https://github.com/cklukas/homebrew-paglets.

set -eu

if [ $# -ne 4 ]; then
  echo "Usage: update-formula.sh VERSION SHA256 OWNER/REPOSITORY OUT.rb" >&2
  exit 2
fi
version=$1
sha256=$2
repository=$3
out=$4
case "$version" in
  [0-9]*.[0-9]*.[0-9]*) ;;
  *) echo "update-formula.sh: version must look like MAJOR.MINOR.PATCH" >&2; exit 1 ;;
esac
case "$sha256" in
  *[!0-9a-f]*|'') echo "update-formula.sh: sha256 must be lowercase hex" >&2; exit 1 ;;
esac
[ ${#sha256} -eq 64 ] || { echo "update-formula.sh: sha256 must be 64 hex characters" >&2; exit 1; }
case "$repository" in
  *[!A-Za-z0-9._/-]*|*/*/*|'') echo "update-formula.sh: repository must be OWNER/NAME" >&2; exit 1 ;;
  */*) ;;
  *) echo "update-formula.sh: repository must be OWNER/NAME" >&2; exit 1 ;;
esac

formula="$(cd "$(dirname "$0")" && pwd)/paglets.rb"
tmp="$out.new"
sed \
  -e "s|^  url \"https://github.com/.*/releases/download/.*\"$|  url \"https://github.com/$repository/releases/download/cpp-v$version/paglets-cpp-$version-source.tar.gz\"|" \
  -e "s|^  sha256 \"[0-9a-f]*\"$|  sha256 \"$sha256\"|" \
  -e "s|^  head \".*\", branch: \"cpp\"$|  head \"https://github.com/$repository.git\", branch: \"cpp\"|" \
  "$formula" >"$tmp"
grep -q "^  sha256 \"$sha256\"$" "$tmp" && grep -q "cpp-v$version/paglets-cpp-$version-source.tar.gz" "$tmp" ||
  { rm -f "$tmp"; echo "update-formula.sh: rewrite failed" >&2; exit 1; }
mv "$tmp" "$out"
printf 'Wrote %s for paglets/cpp %s\n' "$out" "$version"
