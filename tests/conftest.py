from __future__ import annotations

import contextlib
import gzip
from pathlib import Path
from typing import IO, TYPE_CHECKING, BinaryIO

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


def absolute_path(filename: str) -> Path:
    return Path(__file__).parent / filename


def open_file(name: str, mode: str = "rb") -> Iterator[IO]:
    with absolute_path(name).open(mode) as f:
        yield f


def open_file_gz(name: str, mode: str = "rb") -> Iterator[gzip.GzipFile]:
    with gzip.GzipFile(absolute_path(name), mode) as f:
        yield f


def open_files_gz(names: list[str], mode: str = "rb") -> Iterator[list[gzip.GzipFile]]:
    with contextlib.ExitStack() as stack:
        yield [stack.enter_context(gzip.GzipFile(absolute_path(name), mode)) for name in names]


@pytest.fixture
def btrfs_default() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/btrfs-default.bin.gz")


@pytest.fixture
def btrfs_sparse() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/btrfs-sparse.bin.gz")


@pytest.fixture
def btrfs_subvolume() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/btrfs-subvolume.bin.gz")


@pytest.fixture
def btrfs_subvolume_nested() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/btrfs-subvolume-nested.bin.gz")


@pytest.fixture
def btrfs_subvolume_custom_default() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/btrfs-subvolume-custom-default.bin.gz")


@pytest.fixture
def btrfs_subvolume_snapshot() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/btrfs-subvolume-snapshot.bin.gz")


@pytest.fixture
def btrfs_compression() -> Iterator[BinaryIO]:
    yield from open_file_gz("_data/btrfs-compression.bin.gz")


@pytest.fixture
def btrfs_profile_dup() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/btrfs-dup-1.bin.gz", "_data/btrfs-dup-2.bin.gz"])


@pytest.fixture
def btrfs_profile_raid0() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/btrfs-raid0-1.bin.gz", "_data/btrfs-raid0-2.bin.gz"])


@pytest.fixture
def btrfs_profile_raid1() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/btrfs-raid1-1.bin.gz", "_data/btrfs-raid1-2.bin.gz"])


@pytest.fixture
def btrfs_profile_raid1c3() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "_data/btrfs-raid1c3-1.bin.gz",
            "_data/btrfs-raid1c3-2.bin.gz",
            "_data/btrfs-raid1c3-3.bin.gz",
        ]
    )


@pytest.fixture
def btrfs_profile_raid1c4() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "_data/btrfs-raid1c4-1.bin.gz",
            "_data/btrfs-raid1c4-2.bin.gz",
            "_data/btrfs-raid1c4-3.bin.gz",
            "_data/btrfs-raid1c4-4.bin.gz",
        ]
    )


@pytest.fixture
def btrfs_profile_raid5() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/btrfs-raid5-1.bin.gz", "_data/btrfs-raid5-2.bin.gz"])


@pytest.fixture
def btrfs_profile_raid6() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/btrfs-raid6-1.bin.gz", "_data/btrfs-raid6-2.bin.gz", "_data/btrfs-raid6-3.bin.gz"])


@pytest.fixture
def btrfs_profile_raid10() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["_data/btrfs-raid10-1.bin.gz", "_data/btrfs-raid10-2.bin.gz"])
