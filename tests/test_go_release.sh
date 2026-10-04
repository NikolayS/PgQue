#!/usr/bin/env bash
set -Eeuo pipefail

# Exercise the publication script against local mirrors, without credentials.
python3 - <<'PY'
import json
import os
from pathlib import Path
import subprocess
import tempfile

workflow = os.environ.get('PGQUE_GO_RELEASE_WORKFLOW', '.github/workflows/release-go.yml')
steps = json.loads(subprocess.check_output([
    'ruby', '-rjson', '-ryaml', '-e',
    'puts JSON.generate(YAML.load_file(ARGV[0])["jobs"]["publish"]["steps"])',
    workflow,
]))
body = next(s['run'] for s in steps if s.get('name') == 'Push mirror main and tag')
body = body.replace('remote="https://github.com/${MIRROR_REPO}.git"',
                    'remote="$TEST_MIRROR_REMOTE"')

def git(*args):
    return subprocess.check_output(['git', *map(str, args)], stderr=subprocess.DEVNULL).decode().strip()

with tempfile.TemporaryDirectory(prefix='pgque-go-release-') as tmp:
    root = Path(tmp)
    source = root / 'source'
    git('init', '-b', 'main', source)
    git('-C', source, 'config', 'user.name', 'Release test')
    git('-C', source, 'config', 'user.email', 'release-test@example.invalid')
    (source / 'payload').write_text('base\n')
    git('-C', source, 'add', '.')
    git('-C', source, 'commit', '-m', 'base')
    base = git('-C', source, 'rev-parse', 'HEAD')
    (source / 'payload').write_text('release\n')
    git('-C', source, 'commit', '-am', 'release')
    release = git('-C', source, 'rev-parse', 'HEAD')
    git('-C', source, 'checkout', '--detach', base)
    (source / 'payload').write_text('divergent mirror\n')
    git('-C', source, 'commit', '-am', 'divergent')
    divergent = git('-C', source, 'rev-parse', 'HEAD')

    for case in ('success', 'divergent', 'rejected'):
        mirror = root / (case + '.git')
        git('init', '--bare', '-b', 'main', mirror)
        initial = divergent if case == 'divergent' else base
        git('-C', source, 'push', mirror, initial + ':refs/heads/main')
        if case == 'rejected':
            hook = mirror / 'hooks' / 'update'
            hook.write_text('#!/bin/sh\n[ \"$1\" != refs/heads/main ]\n')
            hook.chmod(0o755)
        env = dict(os.environ, VERSION='v0.2.2', MIRROR_REPO='test/mirror',
                   TEST_MIRROR_REMOTE=str(mirror), GITHUB_WORKSPACE=str(source),
                   split_sha=release)
        result = subprocess.run(['bash', '-c', body], env=env, capture_output=True, text=True)
        refs = dict(line.split(' ', 1) for line in git('--git-dir', mirror,
                    'for-each-ref', '--format=%(refname) %(objectname)').splitlines())
        if case == 'success':
            assert result.returncode == 0, result.stderr
            assert refs['refs/heads/main'] == release
            assert git('--git-dir', mirror, 'rev-parse', 'v0.2.2^{}') == release
        else:
            assert result.returncode != 0, case
            assert refs == {'refs/heads/main': initial}, refs
        assert not any('release-candidate/' in r for r in refs), refs
        print('PASS:', case)
PY
