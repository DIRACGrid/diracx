# Splitting pull requests

Large pull requests are the slowest way to get code merged.
As a diff grows, review quality drops: a reviewer can give meaningful feedback on a focused few-hundred-line change, but will inevitably end up skimming a few-thousand-line one.
CI failures become harder to attribute when many changes land at once.
Design discussion gets entangled with line-by-line review, and every round of feedback forces another pass over the whole diff.
Meanwhile the branch accumulates conflicts with `main` faster than it converges towards being mergeable.

This page explains how to think about splitting a large change into a series of smaller pull requests.
It uses a real DiracX example: [#910](https://github.com/DIRACGrid/diracx/pull/910) added a Resource Status System (RSS) source and route in a single PR touching 38 files (+3665/−113).
It stalled with failing integration tests and unresolved design questions, was eventually closed, and was replaced by five focused PRs ([#936](https://github.com/DIRACGrid/diracx/pull/936)–[#940](https://github.com/DIRACGrid/diracx/pull/940)) that were far easier to review.

!!! tip "Write it first, split it afterwards"

    None of this means you need to plan the perfect series of PRs before writing any code.
    It is completely fine — and often the fastest way to understand the problem — to develop the whole feature on a single branch, with a draft PR to run the CI against it.
    Recognising in advance that something deserves to be a common utility takes experience, and guessing at abstractions up front usually means reworking them several times before they fit.
    Let the messy branch teach you where the seams are, then cut the finished work into reviewable PRs once you can see its real shape.
    Splitting is about how the work is *reviewed*, not about how it is *written*.

## How to find the seams

### One reviewable concern per PR

The test to apply is: *could a reviewer hold this whole diff in their head at once?*
A PR should answer a single question, such as "is this cache implementation correct?" or "are these the right database queries?".
If the description needs the word "also", or if reviewing one part requires no knowledge of another part, you have found a seam.

The original #910 mixed at least five separable concerns: a new generic cache, an HTTP caching bug fix, a new source abstraction, new database queries, and the router that tied them together.
A reviewer had to evaluate all of them simultaneously to approve any of them.

### Build the building blocks first

Generic infrastructure should be proposed on its own, before the feature that motivates it.
In the RSS split, [#936](https://github.com/DIRACGrid/diracx/pull/936) added `AsyncTwoLevelCache` (+339 lines, 2 files) and [#938](https://github.com/DIRACGrid/diracx/pull/938) added the `AsyncCacheableSource` abstraction (+491 lines, 7 files).
Both are self-contained, fully tested, and reviewable purely on the question "is this a good building block?" — without any RSS knowledge.

This also produces a better design: infrastructure that has to stand alone in its own PR cannot quietly depend on the internals of the feature it was written for.

### Fixes discovered along the way are separate PRs

While developing a feature you will often find pre-existing bugs or refactoring opportunities.
Resist the temptation to fix them "while you are there".
[#937](https://github.com/DIRACGrid/diracx/pull/937) fixed an `If-Modified-Since` timezone bug and extracted a reusable helper, in a +87/−32 diff that could be reviewed and merged immediately — long before the feature itself was ready.

The conventional-commit type matters here too: a `fix:` buried inside a `feat:` PR is invisible in the release notes and cannot be backported independently.

### Package boundaries are natural seams

DiracX's [repository structure](./repo-structure.md) separates `diracx-db`, `diracx-logic` and `diracx-routers`, and those boundaries usually make good PR boundaries.
[#939](https://github.com/DIRACGrid/diracx/pull/939) contained only the `ResourceStatusDB` query changes and their tests (+327/−75, 4 files).
The reviewer best placed to judge SQL queries is not necessarily the one best placed to judge HTTP caching semantics, and splitting along package lines lets each review happen independently.

This is not a rule that a PR may only touch one package: a change spanning a couple of adjacent layers, such as new queries in `diracx-db` together with the `diracx-logic` that uses them, is perfectly fine.
The warning sign is a PR that touches many packages at once — unless it is a mechanical refactoring, that usually means it is bundling more than one concern.

### Every PR must stand alone

Each PR in the series needs its own description explaining what it does on its own terms, its own tests, and must leave `main` fully working if merged without the rest of the series.
"Preparatory" PRs that add dead code which only makes sense later are a sign the split is in the wrong place.

Difficulty here is useful feedback in itself.
If a piece cannot be tested without standing up the whole feature, or cannot be described without explaining the rest of the series, that usually points at an abstraction or testing approach that needs rethinking — a problem the monolithic PR was hiding rather than avoiding.

## Expressing the relationships between PRs

A split series needs a little bookkeeping so reviewers can navigate it:

- Reference the umbrella issue from every PR ("Part of #839") and, if the series replaces an earlier monolithic PR, say so ("Replaces #910").
- State the dependencies explicitly ("Blocked by #936") so reviewers know the intended merge order and don't merge out of sequence.
- Numbered branch names (`rss-1-async-cache`, `rss-2-http-cache`, …, `rss-5-router`) make the ordering visible at a glance.
- Branch each PR from `main` where possible.
    The final integration PR will initially show the combined diff, but it shrinks to just its own changes as its prerequisites merge.

## The example in full

| PR                                                                                                                                  | Scope                                        | Size              |
| ----------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------- | ----------------- |
| [#936](https://github.com/DIRACGrid/diracx/pull/936) `feat(core): add AsyncTwoLevelCache`                                           | Standalone building block                    | +339/−1, 2 files  |
| [#937](https://github.com/DIRACGrid/diracx/pull/937) `fix(routers): parse If-Modified-Since as GMT and extract apply_cache_headers` | Pre-existing bug found along the way         | +87/−32, 3 files  |
| [#938](https://github.com/DIRACGrid/diracx/pull/938) `feat(core): add AsyncCacheableSource and cacheable-source wiring`             | Generic abstraction (blocked by #936)        | +491/−1, 7 files  |
| [#939](https://github.com/DIRACGrid/diracx/pull/939) `feat(db): bulk all-VO read queries for ResourceStatusDB`                      | Database layer only                          | +327/−75, 4 files |
| [#940](https://github.com/DIRACGrid/diracx/pull/940) `feat: add rss router serving cached resource status`                          | The feature itself, plus regenerated clients | the remainder     |

Four of the five PRs are a few hundred lines and reviewable in one sitting.
The dependency structure also exposes parallelism: #936, #937 and #939 are mutually independent and could be reviewed (and merged) concurrently.

## When not to split

!!! note "Splitting has a cost too"

    Every PR carries overhead: a description, a CI run, a review round, a merge.
    Don't split when:

    - The change is large but mechanical and uniform — a rename or formatting change across many files is *easier* to review as one PR than as twenty.
    - The pieces are meaningless alone — if no individual PR can be described without reference to the others, the split is artificial and just spreads the same review burden over more pages.
    - Generated code must accompany its trigger — regenerating a client in a separate PR from the API change that caused it leaves `main` inconsistent in between.

If in doubt, ask yourself what question each PR asks its reviewer.
One clear question per PR is the goal; zero or three are both signs to re-cut the series.

!!! tip "You don't have to work it out alone"

    Deciding where to cut a change is a skill in itself, and maintainers would much rather help you plan a split than review a 3000-line PR.
    Ask on the related issue or your draft PR, [ask on Mattermost](https://mattermost.web.cern.ch/diracx/channels/developments-and-certifications), or bring it to the [weekly developers meeting](https://indico.cern.ch/category/20884/).
    LLM-based coding tools are also good at this: given the diff of a working branch, they can suggest a sensible series of PRs and handle the mechanical git work of extracting each piece onto its own branch.

See the [contribution guide](../how-to/contribute.md) for the mechanics of opening PRs, commit conventions, and the review process.
