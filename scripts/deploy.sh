#!/usr/bin/env bash

# Based on http://www.debonair.io/post/maven-cd/

# Only runs on merges to master
if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    openssl aes-256-cbc -K $encrypted_2966fe3a76cf_key -iv $encrypted_2966fe3a76cf_iv -in scripts/codesigning.asc.enc -out scripts/codesigning.asc -d
    gpg --fast-import scripts/codesigning.asc
    mvn --settings sonatype-settings.xml -DskipTests -Psign-artifacts deploy
fi
