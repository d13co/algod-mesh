#!/bin/sh
# render.sh - Print the d13co/tap formula for an algod-mesh release.
#
# Syntax:   deploy/homebrew/render.sh <version> <release url> <checksums.txt>
# Example:  deploy/homebrew/render.sh 1.2.3 \
#             https://github.com/d13co/algod-mesh/releases/download/v1.2.3 \
#             dist/checksums.txt > Formula/algod-mesh.rb
#
# The release url is the directory holding algod-mesh-{darwin,linux}-{arm64,amd64},
# and ends in v<version>. checksums.txt holds their sha256sum lines.
set -eu
[ $# -eq 3 ] || { echo "usage: $0 <version> <release url> <checksums.txt>" >&2; exit 2; }
ver=$1 url=${2%/} sums=$3
case $ver in *[!A-Za-z0-9.+~-]*|'') echo "bad version: $ver" >&2; exit 2 ;; esac
case $url in *[!A-Za-z0-9:/._~+-]*|'') echo "bad url: $url" >&2; exit 2 ;; esac
[ -f "$sums" ] || { echo "no checksums file: $sums" >&2; exit 2; }
[ "${url##*/}" = "v$ver" ] || { echo "the url isn't release v$ver's: $url" >&2; exit 2; }

sha() {
	s=$(awk -v f="algod-mesh-$1" '$2 == f || $2 == "*" f || $2 == "./" f { print $1 }' "$sums")
	case $s in *[!0-9a-f]*|'') echo "no single sha256 for algod-mesh-$1 in $sums" >&2; exit 2 ;; esac
	[ ${#s} -eq 64 ] || { echo "bad sha256 for algod-mesh-$1: $s" >&2; exit 2; }
	printf %s "$s"
}
da=$(sha darwin-arm64)
di=$(sha darwin-amd64)
la=$(sha linux-arm64)
li=$(sha linux-amd64)

sed -e "s,@VER@,$ver," -e "s,@URL@,$url,g" \
	-e "s,@SHA_DARWIN_ARM64@,$da," -e "s,@SHA_DARWIN_AMD64@,$di," \
	-e "s,@SHA_LINUX_ARM64@,$la," -e "s,@SHA_LINUX_AMD64@,$li," \
	"$(dirname "$0")/algod-mesh.rb.in"
