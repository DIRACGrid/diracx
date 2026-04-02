This page describes how to mechanically make releases of DiracX and extensions. See the [reference](../reference/making-releases.md) documentation for more details about how releases are made.

# How to make a new DiracX release

Execute the following points in the presented order.

1. Find the `release-please` [pull request](https://github.com/DIRACGrid/diracx/pulls?q=is%3Apr+is%3Aopen+%22chore%28main%29%3A+release%22) for your release. If you can't find it, it means that the currently existing latest release includes already all code changes.

2. Review the release number **and** the content of the changelog

    - If you need to change the release number, follow the [ `release-please` doc](https://github.com/googleapis/release-please?tab=readme-ov-file#how-do-i-change-the-version-number)

    ```bash
    git commit --allow-empty -m "chore: release 0.0.3" -m "Release-As: v0.0.3"
    git push upstream HEAD:main
    ```

    - If you want to update the release notes manually, follow the [`release-please` doc](https://github.com/googleapis/release-please?tab=readme-ov-file#how-can-i-fix-release-notes)

3. Merge the `release-please` PR

4. Check the [deployment action](https://github.com/DIRACGrid/diracx/actions/workflows/deployment.yml?query=event%3Apush+actor%3ADIRACGridBot)

5. When the above will be finished, check that the [CI](https://github.com/DIRACGrid/diracx-charts/actions/workflows/chart-ci.yml?query=actor%3ADIRACGridBot) in `diracx-charts` is successful

At this stage, there is a new `diracx` and `diracx-chart` release.

# How to make a new DiracX-Web release

Execute the following points in the presented order.

1. Find the `release-please` [pull request](https://github.com/DIRACGrid/diracx-web/pulls?q=is%3Apr+is%3Aopen+%22chore%28main%29%3A+release%22) for your release. If you can't find it, it means that the currently existing latest release includes already all code changes.

2. Review the release number **and** the content of the changelog

    - If you need to change the release number, follow the [`release-please` doc](https://github.com/googleapis/release-please?tab=readme-ov-file#how-do-i-change-the-version-number)

    ```bash
    git commit --allow-empty -m "chore: release 0.0.3" -m "Release-As: v0.0.3"
    git push upstream HEAD:main
    ```

    - If you want to update the release notes manually, follow the [`release-please` doc](https://github.com/googleapis/release-please?tab=readme-ov-file#how-can-i-fix-release-notes)

3. Merge the `release-please` PR

4. Check the [deployment action](https://github.com/DIRACGrid/diracx-web/actions/workflows/deployment.yml?query=event%3Apush)

5. When the above is finished, check that the [CI](https://github.com/DIRACGrid/diracx-charts/actions/workflows/chart-ci.yml) in `diracx-charts` is successful

At this stage, there is a new `diracx-web` release and `diracx-chart` release.

## Making the extension release

Making a release of an extension depends on how the extension has been set up. Below are recommended workflows:

=== "Generic GitLab"

    1. Ensure any desired merge requests have been merged.
    2. Ensure the `pyproject.toml` contains the desired DiracX dependency
        - If it does not, run the pipeline schedule to run renovate, which should trigger a merge request to update the version.
        - Merge the renovate-generated MR.
    3. Create tag (and optionally [release](<>)) for your extension
    4. Updates to extension's "charts" repo will be pushed automatically or via merge requests depending on your setup.

=== "LHCb"

    - The LHCb installation follows the "Generic GitLab" workflow.
    - The renovate job runs every Monday at 22:10 UTC. It can be manually triggered [here](https://gitlab.cern.ch/lhcb-dirac/lhcbdiracx/-/pipeline_schedules).
    - The LHCb certification set up is updated automatically, see [here](https://gitlab.cern.ch/lhcb-dirac/lhcbdiracx-charts/-/pipelines?page=1&scope=all&ref=lhcbdiracx-cert) for the pipelines.
    - The LHCb production setup is deployed automatically when the associated merge request is merged. See [here](https://gitlab.cern.ch/lhcb-dirac/lhcbdiracx-charts/-/merge_requests/?sort=created_date&state=opened&author_username=project_217407_bot_a1f6511ecbeab04354572cf37500b0a3&first_page_size=20) for the pending updates.
