# Contribution Guidelines

Hello! Thanks for your interest in joining the mission to accelerate the mass adoption of crypto for personal
sovereignty! We welcome contributions from anyone on the internet.

Note, however, that all the contributions are subject to review, and not every contribution is guaranteed to be merged.
It is highly advised to reach out to developers (for example, by creating an issue) before preparing a significant
change in the codebase, and explicitly confirm that this contribution will be considered for merge. Otherwise, it is
possible to discover that a feature you have spent some time on does not align with the core team vision or capacity to
maintain a high quality of given submission long term.

## Ways to contribute

There are many ways to contribute to the ADI Stack:

1. Open issues: if you find a bug, have something you believe needs to be fixed, or have an idea for a feature, please
   open an issue.
2. Add color to existing issues: provide screenshots, code snippets, and whatever you think would be helpful to resolve
   issues.
3. Resolve issues: either by showing an issue isn't a problem and the current state is ok as is or by fixing the problem
   and opening a PR.

## Fixing issues

To contribute code fixing issues, please fork the repo, fix an issue, commit, add documentation as per the PR template,
and the repo's maintainers will review the PR.
[here](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork)
for guidance how to work with PRs created from a fork.

## Pull request titles and releases

ADI Stack Server has its own version line (`v1.0.0`, `v1.1.0`, …), independent of the
upstream [matter-labs/zksync-os-server](https://github.com/matter-labs/zksync-os-server)
version. The next version is computed from pull request titles, so the title matters:

- Title every pull request as a [Conventional Commit](https://www.conventionalcommits.org/):
  `fix(rpc): retry failed calls`, `feat(prover): add V8 lane`, `docs: …`.
- `fix` and `perf` make a patch release, `feat` a minor one, and `!` after the type
  (`feat!: …`) or a `BREAKING CHANGE:` footer a major one. `chore`, `docs`, `ci`, `build`,
  `test`, `refactor` and `style` release nothing and stay out of the changelog.
- Commits inside the pull request count too: a `feat` commit inside a `fix:` pull request
  makes a minor release.
- An upstream sync (a branch that merges `upstream/main`) is titled
  `<type>(upstream): sync vX.Y.Z`. The type is the bump you decide for our users: `fix(upstream)`,
  `feat(upstream)`, or `feat(upstream)!` when upstream brings a breaking change. The upstream
  commits inside it are not counted one by one. Merge it with a merge commit, never squash.

Every push to `main` runs the [Release workflow](.github/workflows/release.yml). It shows the
next version and changelog entry in the run summary. Releases are not switched on yet, so it
tags nothing. To check the effect of the rules over a range of history, run it by hand with
`from` set to a commit or tag:

```sh
gh workflow run release.yml -f from=<commit or tag>
```

ADI release notes are in [CHANGELOG.adi.md](CHANGELOG.adi.md); the version is in
[VERSION](VERSION). Upstream's `CHANGELOG.md` and the `Cargo.toml` versions are upstream's and
are left as they are.

## Licenses

If you contribute to this project, your contributions will be made to the project under both Apache 2.0 and the MIT
license.

## Resources

We aim to make it as easy as possible to contribute to the mission. This is still WIP, and we're happy for contributions
and suggestions here too. Some resources to help:

1. [In-repo docs aimed at developers](docs)
2. [ZKsync Era docs!](https://docs.adi.foundation/)
3. Company links can be found in the [repositories' readme](README.md)

## Code of Conduct

Be polite and respectful.

## FAQ

**Q**: I have a small contribution that's not getting traction/being merged?

**A**: Due to capacity, contributions that are simple renames of variables or stylistic/minor text improvements, one-off
typo fix will not be merged.

### Thank you
