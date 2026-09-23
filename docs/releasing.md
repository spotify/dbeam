# Releasing DBeam

DBeam publishes `com.spotify:dbeam-core`, `dbeam-bom` and `dbeam-parent` to Maven Central via the
Sonatype Central Portal. Releases are cut by the
[Sonatype Release](https://github.com/spotify/dbeam/actions/workflows/release.yml) workflow, driven
by `maven-release-plugin`.

**Maven Central is immutable.** A published version can never be edited, replaced or deleted. Every
step below exists to catch mistakes before that point.

## Prerequisites

- **Write access** to `spotify/dbeam`. The `main_env` environment has no approval rules, so anyone
  with write access can publish a release unilaterally.
- Nothing to install locally for the normal path — the workflow does everything.

Credentials live as `main_env` environment secrets and are already configured:

| Secret | Used for |
| --- | --- |
| `SONATYPE_USERNAME`, `SONATYPE_TOKEN` | Central Portal user token |
| `GPG_KEY`, `GPG_PASSPHRASE` | Artifact signing |
| `GPG_KEY_NAME` | Only used by the (disabled) deploy job in `maven.yml` |

## Choosing a version number

The `version` input you type is passed as `-DreleaseVersion` and **fully determines what is
published**. Two consequences that surprise people:

- **The `-SNAPSHOT` version in `pom.xml` is ignored.** It is a placeholder, not an input. The
  v0.10.29 release was cut from a tree that said `0.10.28-SNAPSHOT` and published `0.10.29` with no
  warning. You do not need to edit the POM before releasing, and doing so changes nothing.
- **The next development version is derived from your input, not from the POM.** In batch mode the
  plugin increments the last numeric segment of the release version. `0.10.31` leaves master on
  `0.10.32-SNAPSHOT`. There is no workflow input to override this; releasing `0.11.0` lands master
  on `0.11.1-SNAPSHOT`, so if you want a different next line you need a follow-up commit.

There is **no validation** of the input. A typo (`1.0.31`, `0.1.031`) is accepted and published
permanently. Read the value twice before submitting.

Use the version to signal compatibility. 0.10.30 raised `maven.compiler.release` from 8 to 11,
dropping Java 8 support in a patch release — avoid repeating that.

## Step by step

### 1. Pre-flight

- `master` is green in CI.
- Everything you intend to ship is merged. `git log v<last>..master --oneline`.
- Review dependency changes since the last release for anything consumer-visible (a JDK baseline
  change, a major bump in Beam / Avro / SLF4J). These belong in the release notes and may change
  your version number.

### 2. Trigger the release

From the [Actions tab](https://github.com/spotify/dbeam/actions/workflows/release.yml), or:

```shell
gh workflow run release.yml --repo spotify/dbeam -f version=0.10.31
```

**No `v` prefix.** The input sets the POM `<version>` directly. The `v` is added by `tagNameFormat`
(`v@{project.version}`), so `0.10.31` produces tag `v0.10.31`. Typing `v0.10.31` would set the POM
version to `v0.10.31` and the tag to `vv0.10.31`.

Always trigger from `master`. The workflow has no concurrency guard — do not start a second run
while one is in flight.

### 3. Watch it

```shell
gh run watch --repo spotify/dbeam $(gh run list --repo spotify/dbeam --workflow release.yml --limit 1 --json databaseId --jq '.[0].databaseId')
```

Expect ~2 commits and 1 tag pushed to master, then a deploy. If it fails, go to
[Recovery](#recovery) before retrying — a partially completed `release:prepare` leaves state behind.

### 4. Verify on Maven Central

Publication is automatic (`autoPublish=true`); there is no staging repository to close. Artifacts
appear within ~10-30 minutes.

```shell
curl -s https://repo1.maven.org/maven2/com/spotify/dbeam-core/maven-metadata.xml | grep -E '<release>|<latest>'
curl -s https://repo1.maven.org/maven2/com/spotify/dbeam-core/0.10.31/ | grep -o 'dbeam-core[^"]*'
```

Each of `dbeam-core`, `dbeam-bom` and `dbeam-parent` should have its `.pom`, and `dbeam-core` should
additionally have the main jar, `-sources.jar` and `-javadoc.jar`, each with `.asc`, `.md5`, `.sha1`,
`.sha256` and `.sha512`.

> The README previously pointed at `https://oss.sonatype.org/` for this. That host is gone (HTTP
> 404) — legacy OSSRH was sunset in 2025. Use `repo1.maven.org` or the Central Portal.

### 5. Create the GitHub Release

**The workflow does not do this.** The tag is pushed but no release object is created.

```shell
gh release create v0.10.31 --repo spotify/dbeam --verify-tag --generate-notes --latest
```

Then **check the pre-release flag and edit the notes** — see [Known issues](#known-issues) 2 and 3.

### 6. Post-release

- Confirm master is on the expected `-SNAPSHOT` and that `<scm><tag>` still reads `HEAD`.
- Announce if the release carries anything consumer-visible.

## What the workflow actually does

`release.yml` checks out master, configures the `github-actions[bot]` identity, sets up **JDK 11**
and imports the GPG key, then runs a single command:

```shell
mvn -B release:prepare release:perform -DreleaseVersion=<input> -Darguments="-Dgpg.passphrase=..."
```

`release:prepare`:

1. Backs up the POMs to `pom.xml.releaseBackup` and writes `release.properties`.
2. Rewrites `<version>` to the release version in all modules (`autoVersionSubmodules=true`) and
   `<scm><tag>` to `v<version>` (`tagNameFormat`). Commits as `prepare release vX`.
3. Runs the default preparation goals, `clean verify` — the full unit test suite. **The e2e suite
   does not run**; `e2e/e2e.sh` is invoked only by `maven.yml` on PRs and pushes.
4. Tags and pushes.
5. Rewrites `<version>` to the next `-SNAPSHOT` and **restores the `<scm>` section from the backup**.
   Commits as `prepare for next development iteration`.

`release:perform` clones the tag into `target/checkout` and runs `deploy` there with the `release`
profile active (`useReleaseProfile=false`, `releaseProfiles=release`). That profile GPG-signs every
artifact, attaches javadoc, and deploys through `central-publishing-maven-plugin` with
`autoPublish=true`. Tests run a second time here.

Not published: the `pack` profile's shaded fat jar (CI-only) and `-SNAPSHOT` builds (the
`snapshotRepository` is commented out in `distributionManagement`).

## Known issues

### 1. The `version` input is unvalidated and Central is immutable

The single highest-risk step. Nothing checks that your input is well-formed or greater than the
current version. See [Recovery](#recovery) — there is no undo.

### 2. The GitHub Release is not created, and the pre-release flag is a repeat offender

The workflow pushes a tag and stops. When the release is created by hand it has twice been left
flagged as a pre-release — PR #1011 was literally titled "revert prerelease", and v0.10.30 sat
flagged for three months, so the releases page and the README tag badge advertised v0.10.29 while
Central served v0.10.30. Pass `--latest` and verify with:

```shell
gh release view v0.10.31 --repo spotify/dbeam --json tagName,isPrerelease,isLatest
```

### 3. Auto-generated release notes only list pull requests

`--generate-notes` enumerates merged PRs. When a release cycle is squashed into one large PR the
notes collapse to a single line — v0.10.30's ~30 dependency, JUnit 5 and Java 25 commits all landed
in #1039, and the generated notes were one bullet that mentioned neither the Java 8 drop nor the
SLF4J 2.x migration. For a release like that, write the notes from `git log` instead.

### 4. `<scm><tag>` is a self-perpetuating loop

The development phase **restores** `<scm><tag>` from the backup POM rather than computing it, so
whatever master holds, master keeps holding. Master must hold the sentinel `HEAD`. During the
v0.10.29 cycle a revert put the literal `v0.10.29` there and it was replayed through v0.10.30
(fixed in #1044).

This matters because a standalone `release:perform` resolves its checkout from `release.properties`
and then falls back to the POM's `<scm>` section — with a stale tag it would check out the wrong
commit and try to redeploy an already-published version. **If you ever revert a release commit,
check that `<scm><tag>` is back to `HEAD`.**

### 5. The `v0.10.29` tag does not point at the released commit

It points at `e032ae1`, but the published 0.10.29 artifacts were built from `558b87a` (the Central
POM contains `central-publishing-maven-plugin`, added later in #1012). Do not trust
tag-to-artifact correspondence for that one version, and treat `v0.10.29...vX` compare ranges as
over-reporting. The tag was left in place deliberately: force-moving a published tag breaks anyone
who pinned it.

### 6. Release builds on JDK 11, but v0.10.30 was cut locally on JDK 21

v0.10.30's manifest shows `Build-Jdk-Spec: 21` because it was released from a laptop rather than
through the workflow. The bytecode target is unaffected (`maven.compiler.release=11`), but for
reproducible provenance, cut releases through the workflow.

### 7. Signing depends on one person's personal key

Artifacts are signed with RSA-4096 key `E372CF3377C63C2F79EC4D13464754B85A79AB63`
(`Luis Bianchin <labianchin@spotify.com>`, created 2021-04-16, no expiry). It does not expire, but
it is tied to an individual rather than to the project.

### 8. Stale configuration to ignore or clean up

- `maven.yml`'s `deploy` job is permanently disabled (`if: false`) and references a
  `github-settings.xml` that does not exist in the repo.
- `sonatype-settings.xml`'s header comment describes Travis and `.travis.yml`; neither exists.
- `nexus-staging-maven-plugin` is still declared solely to disable itself, which keeps attracting
  Dependabot PRs (#1013).
- `.github/release-drafter.yml` exists with no workflow to run it, so it does nothing.
- `distributionManagement` still points at the dead `oss.sonatype.org` endpoints; deployment
  actually goes through `central-publishing-maven-plugin`, so these are inert.

## Recovery

**A published version cannot be withdrawn.** With `autoPublish=true` there is no staging window in
which to drop a deployment. If a bad version reaches Central, the only remedy is to publish a
corrected one and document the bad version in its release notes.

Before it reaches Central, the release is recoverable. From a local clone:

```shell
# Undo prepare's POM rewrites and commits (only works while release.properties is present)
mvn release:rollback

# Remove the tag if prepare pushed it
git tag -d v0.10.31 && git push --delete origin v0.10.31

# Reset master if commits were pushed
git reset --hard <sha-before-prepare> && git push --force-with-lease origin master

# Clear leftover plugin state
mvn release:clean
```

After any such revert, verify two things before retrying: `<version>` is the intended `-SNAPSHOT`,
and `<scm><tag>` is `HEAD` (known issue 4).

If the tag was pushed and the build succeeded but the deploy failed, you can retry just the deploy
rather than redoing the whole cycle:

```shell
mvn release:perform -Dtag=v0.10.31 -DconnectionUrl=scm:git:https://github.com/spotify/dbeam.git
```

## Manual release (fallback)

Only for when the workflow itself is broken. Requires the GPG private key locally plus
`SONATYPE_USERNAME`, `SONATYPE_PASSWORD` (a Central Portal
[user token](https://help.sonatype.com/en/user-tokens.html)) and `MAVEN_GPG_KEY_NAME` in the
environment.

```shell
mvn -s sonatype-settings.xml -DreleaseVersion=0.10.31 -DdryRun=true release:prepare  # validate first
mvn -s sonatype-settings.xml -DreleaseVersion=0.10.31 release:prepare release:perform
```

Note this differs from the workflow: without `-B` the plugin runs **interactively** and will prompt
for the development version, and the build uses whatever JDK you have locally.
