# dissect.btrfs

A Dissect module implementing a parser for the Btrfs file system, a commonly used Linux filesystem. For more
information, please see [the documentation](https://docs.dissect.tools/en/latest/projects/dissect.btrfs/index.html).

## Requirements

This project is part of the Dissect framework and requires Python.

Information on the supported Python versions can be found in the Getting Started section of [the documentation](https://docs.dissect.tools/en/latest/index.html#getting-started).

## Installation

`dissect.btrfs` is available on [PyPI](https://pypi.org/project/dissect.btrfs/).

```bash
pip install dissect.btrfs
```

This project decompresses lzo compressed file systems and can use the faster, native (C-based) lzo implementation when
installed, instead of the slower pure Python implementation provided by `dissect.util`. To use these faster
implementations, install the package with the lzo extra:

```bash
pip install "dissect.btrfs[lzo]"
```

Unfortunately there is no binary `python-lzo` wheel for PyPy installations on Windows, so it won't be installed there.

This module including the lzo extra is also automatically installed if you install the `dissect` package.

## Build and test instructions

This project uses `tox` to build source and wheel distributions. Run the following command from the root folder to build
these:

```bash
tox -e build
```

The build artifacts can be found in the `dist/` directory.

`tox` is also used to run linting and unit tests in a self-contained environment. To run both linting and unit tests
using the default installed Python version, run:

```bash
tox
```

For a more elaborate explanation on how to build and test the project, please see [the
documentation](https://docs.dissect.tools/en/latest/contributing/tooling.html).

## Contributing

The Dissect project encourages any contribution to the codebase. To make your contribution fit into the project, please
refer to [the development guide](https://docs.dissect.tools/en/latest/contributing/developing.html).

## Copyright and license

Dissect is released as open source by Fox-IT (<https://www.fox-it.com>) part of NCC Group Plc
(<https://www.nccgroup.com>).

Developed by the Dissect Team (<dissect@fox-it.com>) and made available at <https://github.com/fox-it/dissect>.

License terms: AGPL3 (<https://www.gnu.org/licenses/agpl-3.0.html>). For more information, see the LICENSE file.
