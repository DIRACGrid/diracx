# Writing documentation

This page guides you through how to contribute to the DiracX documentation to make significant changes.
If you are only making a small edit, we recommend using the edit buttons on top right of each page of the documentation and using the GitHub web interface.

## Step 1: Think about who you're writing documentation for

In the DiracX documentation we have three distinct audiences:

- Developers: The people who edit the code of DIRAC itself.
- Admins: The people who maintain installations DIRAC for one or more virtual organizations.
- Users: The people who actually submit jobs and access data via DIRAC.

Some people fit into more that one group however when writing documentation you should always start by figuring which audience you expect to read it.

## Step 2: Think about what kind of documentation you're writing

In DiracX we follow the The Grand Unified Theory of Documentation for deciding how to separate documentation into four general categories.
You can read more about the system [here](https://docs.divio.com/documentation-system/).
At first it can be easy to mix up the various categories so uf you're uncertain about the type of documentation you want to write please open a GitHub issue to get advice.

## Step 3: Preparing your local environment

In order to go any further you'll need to have a [local development environment](../tutorials/getting-started.md).
This website is build from a mixture of the `docs/` folders from the three repositories, combined with a custom [mkdocs plugin](https://github.com/DIRACGrid/mkdocs-diracx-plugin).
If you're writing documentation for the DiracX web application or the DiracX helm chart you'll also need a local clone of [diracx-web](https://github.com/DIRACGrid/diracx-web) and/or [diracx-charts](https://github.com/DIRACGrid/diracx-charts).
To configure `mkdocs` to use your local clones of those repos, edit the end of the `mkdocs.yml` as follows:

```diff title="<code>mkdocs.yml</code>"
 plugins:
   - diracx:
       repos:
-        - url: https://github.com/DIRACGrid/diracx-charts
+        - url: /path/to/diracx-charts
           branch: master
           include:
             - docs
             - diracx
-        - url: https://github.com/DIRACGrid/diracx-web
+        - url: /path/to/diracx-web
           branch: main
           include:
             - docs
```

You can then start running a local `mkdocs` server with:

```bash
pixi run mkdocs
```

This command will print the URL you can connect to to view your locally running instance of the documentation, typically [http://127.0.0.1:8000/](http://127.0.0.1:8000/).
When you make save changes to the pages will automatically reload.

## Step 4: Adding pages

To add a new page, create the markdown files in the appropriate directory and then add the file to the `nav:` section of the `mkdocs.yml`.

!!! note

    The navigation structure is only stored in the base `diracx` repository, even for pages which are stored within the `diracx-charts`/`diracx-web` repositories.

## Step 5: Styling

The DiracX documentation is written in [markdown](https://daringfireball.net/projects/markdown/basics) with a variety of extensions to facilitate more complex styling.
New documentation should be consistent with the style of the existing DiracX documentation and a reference on how to use the various markdown extensions can be found [here](https://squidfunk.github.io/mkdocs-material/reference/).

## Step 6: Storing images and other large files

As the documentation is stored in the main Git repositories it is important to avoid committing large binary files.
To achieve this we have a a separate website for hosting these assets (https://diracx-docs-static.s3.cern.ch/assets).
To add content, include the assets in your issue or pull request and the DIRAC developers will upload the files for you.

??? note "Adding content for DIRAC developers"

    Assuming you have credentials configured according to the [CERN documentation](https://clouddocs.web.cern.ch/object_store/s3cmd.html) files can be added with:

    ```bash
    s3cmd put -P path/to/image.png s3://diracx-docs-static/assets/images/path/to/image/
    ```
