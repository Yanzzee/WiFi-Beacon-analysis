# Synchronizing the Alpha 0.2 documentation cleanup

These commands cover this documentation/ignore cleanup only. The public main
branch already contains the accepted Alpha 0.2 application snapshot. Run each
block in order in the same PowerShell session and stop if a command fails.
Review any pre-existing staged changes before committing; these commands
deliberately leave local `config.env` edits and legacy sample CSV deletions out.

The shared path list is explicit. It contains no `example-reports/` directory,
so restoring it cannot remove or overwrite the published report assets. A
whole-tree restore or a merge of application main into the report branch is
unnecessary for this cleanup. Report-site edits remain on their own branch.

## 1. Commit development and fast-forward development-repo main

```powershell
$releaseDocs = @('.gitignore', 'README.md', 'docs', 'tests/fixtures/README.md', 'tests/fixtures/corrected_semantics/README.md', 'tests/golden/README.md')
Set-Location C:\Users\yanzzee\Documents\GitHub\Beacon-analysis
git switch development
git add -- $releaseDocs
git rm --cached --ignore-unmatch -- .DS_Store sample/.DS_Store
git diff --cached --stat
git commit -m "Finalize Alpha 0.2 documentation and ignore cleanup"
git push origin development
git switch main
git merge --ff-only development
git push origin main
```

`.DS_Store` is ignored at every depth. The `--cached` removal stops tracking
the two known entries; it does not delete local files. Missing entries are
accepted. Other sample-file deletions and machine-specific configuration edits
are not staged by the explicit path list. Use the staged diff to confirm that
before committing. An already completed block can result in “nothing to commit.”

## 2. Copy only the cleanup to public main

```powershell
Set-Location C:\Users\yanzzee\Documents\GitHub\WiFi-Beacon-analysis
git switch main
git pull --ff-only origin main
git fetch --no-tags ..\Beacon-analysis main
git restore --source=FETCH_HEAD --staged --worktree -- $releaseDocs
git rm --cached --ignore-unmatch -- .DS_Store sample/.DS_Store
git diff --cached --stat
git commit -m "Sync Alpha 0.2 documentation cleanup"
git push origin main
```

This creates a normal commit on the existing public history. It does not
merge the development repository's history or restore its sample/input data.
There is no force push. The fetch reads the local development repository, so
it does not depend on the public repo's older `dev-repo` remote-tracking refs.

## 3. Copy only the cleanup to the published report branch

The existing `WiFi-Beacon-analysis-readme-sync` worktree uses branch
`sync-example-readme`, based on the current remote `example-reports` branch.
It is separate from the old, locally prepared `example-reports` worktree under
the development repo's ignored `output/previews/` directory. The latter is not
the current published branch tip and retains an earlier local HTML edit.

```powershell
Set-Location C:\Users\yanzzee\Documents\GitHub\WiFi-Beacon-analysis-readme-sync
git pull --ff-only origin example-reports
git restore --source=origin/main --staged --worktree -- $releaseDocs
git rm --cached --ignore-unmatch -- .DS_Store sample/.DS_Store
git diff --cached --name-only
git commit -m "Sync Alpha 0.2 documentation cleanup"
git push origin HEAD:example-reports
```

`origin/main` is shared across the public repo's worktrees and is updated by
the preceding main-branch push. The commit should contain only the named
documentation/ignore paths and removal of tracked Finder metadata. The report
folder, its landing page, figures, downloads, `.nojekyll` and report-specific
Git attributes are not part of the restore. If the pull cannot fast-forward,
stop and inspect the branch divergence instead of replacing remote history.

Pages remains configured for branch `example-reports`, folder `/(root)`, with
the landing page at
<https://yanzzee.github.io/WiFi-Beacon-analysis/example-reports/>.
These commands do not create or move a release tag. Tag/GitHub Releases metadata
can be published separately for the accepted release commit.
