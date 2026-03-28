#!/bin/bash
set -e

# shellcheck disable=SC1091
. ./release.env

gh release create "$RELEASE_VERSION" --title "$RELEASE_TITLE" --notes "${RELEASE_NOTE}" --latest
