# Getting started

This guide will help you make your first changes in DiracX.

## Pre-requisites

Before starting we suggest ensuring you've installed:

<div class="annotate" markdown>
* **pixi:** We recommend using `pixi` (1) to manage your development environment, see the upstream [installation instructions](https://pixi.sh/latest/advanced/installation/).
* **A code editor installed locally:** There are no firm requirements here and it's a question of personal preference. If you have no pre-existing preferences, we would recommend [Visual Studio Code](https://code.visualstudio.com/docs/getstarted/getting-started).
* **An Unix-like operating system:** The DiracX tests are only officially supported on Unix-like operating systems like macOS and Linux. If you're running Windows we would suggest using the [Windows Subsystem for Linux](https://learn.microsoft.com/en-us/windows/wsl/install).
</div>

1. :man_raising_hand: **Why pixi and not conda?**

    Pixi uses conda and conda-forge as it's source of packages so we're still using conda.

    :man_raising_hand: **Then why not use `conda`/`mamba`/`micromamba` to install the packages?**

    Pixi provides a workspace-orientated view rather than deferring the management of environments to the user.
    This allows us to have greater cohesion between local development and the CI and means developers only have to run a single command:

    ```bash
    pixi run pytest-diracx
    ```

    Pixi takes care of creating any environments you need in the `.pixi` directory of the repo.

    This is especially useful for cases where multiple environments are needed like generating the client with Autorest or running the tests for gubbins.

We also recommend being familiar with:

- **Git and the GitHub pull-request workflow:** See the [official GitHub tutorial](https://docs.github.com/en/get-started/start-your-journey/hello-world) if you're unfamiliar with Git and/or GitHub.

## Getting DiracX locally

First clone your fork of the DiracX repo:

=== "SSH"

    ```bash
    git clone git@github.com:<GitHub username>/diracx.git
    cd diracx
    git remote add upstream git@github.com:DIRACGrid/diracx.git
    ```

=== "GitHub CLI"

    ```bash
    gh repo fork DIRACGrid/diracx # (1)!
    ```

    1. The GitHub CLI can be installed as a [`pixi global` tool](https://pixi.sh/dev/global_tools/introduction/) using:

        ```bash
        pixi global install gh
        ```

=== "HTTPS"

    ```bash
    git clone https://github.com/<GitHub username>/diracx.git
    cd diracx
    git remote add upstream https://github.com/DIRACGrid/diracx.git
    ```

DiracX uses [`pre-commit`](https://pre-commit.com/) to format code and check for issues.
The easiest way to use `pre-commit` is to run the following after cloning:

```bash
pixi run pre-commit install
```

This will result in pre-commit being ran automatically each time you run `git commit`.
If you want to explicitly run pre-commit you can use:

```bash
pixi run pre-commit # (1)!
pixi run pre-commit --all-files # (2)!
```

1. Runs `pre-commit` only for files which are uncommitted or which have been changed.
2. Runs `pre-commit` for all files even if you haven't changed them.

## Running the tests

Once you have a local clone of the [`diracx`](https://github.com/DIRACGrid/diracx) repo you can run the tests using:

```bash
pixi run pytest-diracx
```

This will run the basic tests of all DiracX components in a suitable environment.
You can also pass additional arguments to `pytest`:

```bash
pixi run pytest-diracx -k 'test_get_token and not lock_file' # (1)!
pixi run pytest-diracx --pdb # (2)!
```

1. Only run tests with "`test_get_token`" in their name, except for `test_get_token_accessing_lock_file`.

    See [here](https://docs.pytest.org/en/stable/example/markers.html#using-k-expr-to-select-tests-based-on-their-name) for details.

2. When a test fails, launch an interactive `pdb` session.

    See [here](https://docs.pytest.org/en/stable/how-to/failures.html#using-python-library-pdb-with-pytest) for details.

## Your first mission

You already have enough available to do most developments.

As an example we're going to add a new sub-command to `diracx-cli`:

```bash title="<code>$ dirac config show-joke</code>"
Why did the 🐣 cross the road?
Press Enter to see the answer... # (1)!
```

1. You'll have to finish this tutorial to find out!

## The structure of the DiracX repository

The DiracX package is arranged as a "[namespace package](https://packaging.python.org/en/latest/guides/packaging-namespace-packages/)".
In summary this means that each import under `diracx`, e.g. `diracx.core` is distributed as a different package.
__*Why?*__ To allow users to install only part of the DiracX functionality, e.g. only server's have `diracx-db` installed and only clients have `diracx-cli`.

For this development we only need to modify the code in `diracx.cli` so we know the sources are inside:

```bash
diracx-cli/src/diracx/cli/
```

## Adding the code for your feature

To add our `show-joke` command we can append `diracx-cli/src/diracx/cli/config.py` to add:

```bash
import base64

def reveal_answer(encoded: str) -> str:
    return base64.b64decode(encoded.encode()).decode("utf-8")

@app.async_command()
async def show_joke():
    print("Why did the 🐣 cross the road?")
    input("Press Enter to see the answer...")
    print(reveal_answer("VG8gZ2V0IHRvIHRoZSBvdGhlciBzaWRlISDwn5iK"))
```

!!! question "How did we know that a new subcommand is added using the `@app.async_command()` decorator?"

    In the [components explanation](../explanations/components/index.md) the `diracx-cli` section explains how [Typer](https://typer.tiangolo.com/) is used to define the CLI.

    In addition, there is a dedicated how-to for [adding a CLI command](../how-to/add-a-cli-command.md).

## Running the code

For some simple functionality like the `show-joke` command you can easily run the code locally without needing any pre-requisites like a running server.

To do this you can use `pixi run` to run a command in the `default` environment:

```bash title="<code>$ pixi run -e default -- dirac config show-joke</code>"
Why did the 🐣 cross the road?
Press Enter to see the answer...
```

Alternatively, you can start a shell inside the environment:

```bash title="<code>$ pixi shell -e default</code>"
$ which dirac
$PWD/.pixi/envs/default/bin/dirac

$ dirac config show-joke
Why did the 🐣 cross the road?
Press Enter to see the answer...

$ python -c 'import diracx.cli; print(diracx.cli)'
<module 'diracx.cli' from '$PWD/diracx-cli/src/diracx/cli/__init__.py'>
```

More information about the available environments can be found [here](../reference/pixi-tasks.md).

## Writing a test

So now you've seen the code work, but how can we write a test to ensure it keeps working?

Simply add a [`pytest`](https://pytest.org/) test the `diracx-cli/tests/` directory.
This could be included in a pre-existing test file however in this case we're going to add a dedicated file for the test.

```python title="<code>diracx-cli/tests/test_joke.py</code>"
from __future__ import annotations

from typer.testing import CliRunner

from diracx.cli import app

runner = CliRunner()


def test_show_joke():
    result = runner.invoke(app, ["config", "show-joke"], input="\n")  # (1)!
    assert result.exit_code == 0, result.output
    assert "Why did the 🐣 cross the road?" in result.stdout
    assert "<expected answer>" in result.stdout  # (2)!
```

1. DiracX is designed to be easily testable, see the [testing reference](../reference/test-recipes.md) for examples of how to write fast, reliable tests using the various fixtures and helper functions we have available in DiracX.
    In this case, we're following the standard pattern for testing a `typer` CLI that is [documented upstream](https://typer.tiangolo.com/tutorial/testing/).

2. This assertion is expected to fail as the answer to the joke isn't `<expected answer>` 😉.

To run only tests with "`joke`" in their names:

```bash
pixi run pytest-diracx -k joke
```

You should now see this test fail with the fact `<expected answer>` does not appear in the result.
If you edit the test to check for the expected answer you should see the test succeed.

## Committing your work

At this point you can follow the standard git workflow for adding your changes:

```bash
git add diracx-cli/src/diracx/cli/config.py diracx-cli/tests/test_joke.py
git commit -m 'feat(cli): add "diracx config show-joke" CLI command' # (1)!
```

1. The commit message should follow the [Conventional Commits](https://www.conventionalcommits.org/) specification.
    See the [contributing documentation](../how-to/contribute.md) for more details.

It's likely that the commit failed the pre-commit hook with errors like:

```bash
diracx-cli/src/diracx/cli/config.py:26:1: E402 Module level import not at top of file
```

and

```bash
All done! ✨ 🍰 ✨
1 file reformatted, 1 file left unchanged.
```

These are expected and are likely to be fixed automatically, in this case you can see the diff before adding the changes:

```bash title="<code>git diff</code>"
diff --git a/diracx-cli/src/diracx/cli/config.py b/diracx-cli/src/diracx/cli/config.py
index f1b6aed..6b1c472 100644
--- a/diracx-cli/src/diracx/cli/config.py
+++ b/diracx-cli/src/diracx/cli/config.py
@@ -25,9 +25,11 @@ async def dump():

 import base64

+
 def reveal_answer(encoded: str) -> str:
     return base64.b64decode(encoded).decode("utf-8")

+
 @app.async_command()
 async def show_joke():
     print("Why did the 🐣 cross the road?")
```

In this case when we try to commit we still see an additional error:

```bash title="<code>git commit -m 'feat(cli): add &quot;diracx config show-joke&quot; CLI command'</code>"
ruff.....................................................................Failed
- hook id: ruff
- exit code: 1

diracx-cli/src/diracx/cli/config.py:26:1: E402 Module level import not at top of file
   |
26 | import base64
   | ^^^^^^^^^^^^^ E402
   |

Found 1 error.
```

This is not fixed automatically as the pre-commit configuration tries to only automatically apply "safe" fixes.
In this case we know that the import can be moved to the top of the file, at which point `pre-commit` should allow you to commit.

**Important:** Each time `git commit` fails you must explicitly use `git add` to pick up any changes made by pre-commit.

!!! tip "Help! I'm completely stuck and can't figure out what's wrong"

    If you're unable to understand the problem reported by `pre-commit` you can pass `--no-verify`, i.e.

    ```bash
    git commit -m 'feat(cli): add "diracx config show-joke" CLI command' --no-verify
    ```

    This will cause `git` to skip running the pre-commit hooks and reviewers of your contributions to DiracX can look at the [CI](https://docs.github.com/en/actions/about-github-actions/about-continuous-integration-with-github-actions#about-continuous-integration) to help you understand the issue.

## Next steps

Congratulations! You now know all of the basics of how to develop DiracX.
If you're not sure what to do next we recommend:

- More information about how to [contribute to DiracX](../how-to/contribute.md)
- Read about the [design of DiracX](../how-to/contribute.md)
- Learn how-to [run a fully featured DiracX instance locally](../tutorials/run-locally.md).
- Search this documentation for terms related to what you want to develop (e.g. "add route").
