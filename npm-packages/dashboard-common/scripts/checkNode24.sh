#!/usr/bin/env bash

if [[ "$(node -p 'process.versions.node.split(".")[0]')" != "24" ]]; then
  echo "Node version must be 24 to lint this package. Run \`nvm use\` to switch to the version in .nvmrc."
  exit 1
fi
