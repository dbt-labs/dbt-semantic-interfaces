# Contributing to `dbt-semantic-interfaces`

Welcome to `dbt-semantic-interfaces`! We're excited to have you here. This document will help you get started with contributing to the project.

## Before you get started, please...

1. Familiarize yourself with our [Code of Conduct](https://www.getdbt.com/community/code-of-conduct/#:~:text=We%20want%20everyone%20to%20have,don't%20be%20a%20jerk.). In summary - be kind to each other. We're all here trying to make the data world a better place to work.
2. Make sure you can sign our [Contributor License Agreement](https://docs.getdbt.com/community/resources/contributor-license-agreements). Unfortunately, we cannot accept PRs unless you have signed. If you are not able to sign the agreement you may still participate in our Slack community or interact with Issues. To sign the agreement simply put up a PR, and you will receive instructions there.

## Environment setup

1. Ensure you have Python >= `3.8`.
2. [Create a fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) of the [repo](https://github.com/dbt-labs/dbt-semantic-interfaces) and [clone it locally](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository).
3. Set up your environment using `hatch`. We provide the following two commands to help you get set up:
    - `make install-hatch`: Installs hatch
    - `make install` : uses hatch to install all the dependencies
4. Begin developing!

## Start testing and development

You're ready to start! Note all `make` commands should be run from your repository root unless otherwise indicated.

1. Run some tests to make sure things happen:
    - Run the full test suite: `make test`
    - Run a subset of tests based on path: `hatch run pytest TODO: DIRECTORY PATH`
2. Now you may wish to break some tests. Make some local changes and run the relevant tests again and see if you broke them!
3. Run the linters with `make lint` at any time, but especially before submitting a PR. We use:
    - `Black` for formatting
    - `Ruff` for general Python linting
    - `MyPy` for typechecking

## Adding or modifying a CHANGELOG Entry!

We use [changie](https://changie.dev) to generate `CHANGELOG` entries. **Note:** Do not edit the `CHANGELOG.md` directly. Your modifications will be lost.

In order to use it, you can:

1. Follow the steps to [install `changie`](https://changie.dev/guide/installation/) for your system.
2. Once changie is installed and your PR is created for a new feature, run the following command and changie will walk you through the process of creating a changelog entry. `changie new`.
3. Commit the file that's created and your changelog entry is complete!
4. (Optional if contributing to a feature in progress) Modify the changie yaml file in `dbt-semantic-interfaces/.changes/unreleased/` related to your change. If you need help finding this file, please ask within the discussion for the pull request!

You don't need to worry about which `dbt-semantic-interfaces` version your change will go into. Just create the changelog entry with `changie`, and open your PR against the `main` branch. All merged changes will be included in the next minor version of `dbt-semantic-interfaces`. The maintainers _may_ choose to "backport" specific changes in order to patch older minor versions. In that case, a maintainer will take care of that backport after merging your PR, before releasing the new version of `dbt-semantic-interfaces`.

## Submit your contribution!

1. Merge your changes into your fork of the `dbt-semantic-interfaces` repository
2. Make a well-formed Pull Request (PR) from your fork into the main repository. If you're not clear on what a well-formed PR looks like, fear not! We provide a structure to follow.
3. One of our core contributors will review your PR and either approve it or send it back with requests for updates
4. Once the PR has been approved, our core contributors will merge it into the main project.
5. You will get a shoutout in our changelog/release notes. Thank you for your contribution!