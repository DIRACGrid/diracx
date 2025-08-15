# Contributing to DiracX

### 1. Open an Issue

- **Discuss Before Implementing:** Before making a pull request (PR), especially for non-trivial changes, please [open an issue](https://github.com/DIRACGrid/diracx/issues) to discuss your idea. This ensures that everyone is aligned on the proposed change.
- **Check for Existing Issues:** Before opening a new issue, please check if a similar issue already exists. If a similar issue exists, consider contributing to the discussion there instead.

**Good to know:** If you want to start contributing right away, check out the issues labeled with ["good first issue"](https://github.com/DIRACGrid/diracx-web/labels/good%20first%20issue). These are issues that - in principle - are well-suited for newcomers to the project.

### 2. Make Changes

- **Fork the Repository:** Start by forking the repository and creating a new branch for your work. Use a descriptive name for your branch that reflects the work you are doing.

=== "DiracX"

    **Requirements:** [Getting Started](../tutorials/getting-started.md)

    TODO

=== "DiracX Web"

    **Requirements:** [Getting Started](../tutorials/getting-started.md)

    - **Code Documentation:** Ensure that any code you write is well-documented. This includes:

        - Inline comments where necessary to explain complex logic.
        - Updating or creating Storybook documentation if you are contributing to the `diracx-web-components` library.

    - **Writing/Updating Tests:** When you change or add new code, make sure to write or update tests accordingly. This helps maintain the reliability and stability of the codebase.

    **Note:** Don't forget to update the `extensions` code if you integrate breaking changes in the `diracx-web-components` library. See [Managing the extension](../setup_environment.md) for further details.

### 3. Commit

- **Conventional Commits:** All commits must follow the [Conventional Commits](https://www.conventionalcommits.org/) specification. This ensures that commit messages are structured and consistent, which is important for automation and versioning.

    - **Examples:**
        - `feat(cli): add transformation debug command`
        - `feat(ui): add new button component`
        - `fix(api): handle null values in response`
        - `docs(readme): update contributing guidelines`
    - **Why?** If your commit messages do not follow this convention, the Continuous Integration (CI) process will fail, and your PR will not be merged. Please ensure your commit messages are properly formatted before pushing.

- **Note**: We recommend using git hooks to automatically apply some validation to the commits. In `diracx`/`diracx-charts` we use [`pre-commit`](https://pre-commit.com/). In `diracx-web`, we use [`Husky`](https://typicode.github.io/husky/) is configured to run as a pre-commit hook, executing tasks such as linting staged files to maintain code consistency with the codebase.

### 4. Make a Pull Request (PR)

- **Submit Your PR:** When you’re ready, submit your pull request. Please include a clear description of what your PR does and reference the issue number it addresses (if applicable).
- **Review Process:** Your PR will be reviewed by project maintainers. Please be patient and responsive to any feedback you receive.

### 5. Additional Notes

- **Trivial Changes:** For minor changes like fixing typos, feel free to skip the issue creation step and go straight to making a PR.
- **Stay Up-to-Date:** Make sure your branch is up-to-date with the latest changes in the main branch before submitting your PR. Use `git rebase` if necessary.
