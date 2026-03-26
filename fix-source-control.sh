#!/bin/bash
# Fix Source Control visibility for all repos in the workspace.
# Run this from Terminal.app AFTER fully quitting Cursor (Cmd+Q).

DB="$HOME/Library/Application Support/Cursor/User/workspaceStorage/9b1a9c92c835a297c2e2ad73083343d8/state.vscdb"

echo "Before fix:"
sqlite3 "$DB" "SELECT value FROM ItemTable WHERE key = 'scm:view:visibleRepositories';"

sqlite3 "$DB" "UPDATE ItemTable SET value = '{\"all\":[\"git:Git:file:///Users/akurz/alexhkurz-at-git/substack-cartographer\",\"git:Git:file:///Users/akurz/alexhkurz-at-git/substack-cartographer-private\",\"git:Git:file:///Users/akurz/alexhkurz-at-git/substack_api\",\"git:Git:file:///Users/akurz/alexhkurz-at-git/substack-pAIa\"],\"sortKey\":\"discoveryTime\",\"visible\":[0,1,2,3]}' WHERE key = 'scm:view:visibleRepositories';"

echo ""
echo "After fix:"
sqlite3 "$DB" "SELECT value FROM ItemTable WHERE key = 'scm:view:visibleRepositories';"
echo ""
echo "Done. Now open Cursor."
