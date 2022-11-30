#!/bin/bash

if ! dpkg-query -Wf'${Status}' nodejs 2>/dev/null | grep -q '^i'
then
  echo "Installing nodejs..."
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends nodejs || exit 1
  sudo rm -rf /var/lib/apt/lists/*
fi

exit 0

