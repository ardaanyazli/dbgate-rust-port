#!/bin/sh
# Fake native client for route tests: echoes its arguments so tests can
# assert the exact flag order `backup_native`/`restore_native` pass through.
echo "$@"