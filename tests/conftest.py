import contextlib
import gzip
import os
from typing import IO, BinaryIO, Iterator

import pytest


def absolute_path(filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), filename)


def open_file(name: str, mode: str = "rb") -> Iterator[IO]:
    with open(absolute_path(name), mode) as f:
        yield f


def open_file_gz(name: str, mode: str = "rb") -> Iterator[gzip.GzipFile]:
    with gzip.GzipFile(absolute_path(name), mode) as f:
        yield f


def open_files_gz(names: list[str], mode: str = "rb") -> Iterator[list[gzip.GzipFile]]:
    with contextlib.ExitStack() as stack:
        yield [stack.enter_context(gzip.GzipFile(absolute_path(name), mode)) for name in names]


@pytest.fixture
def btrfs_default() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/btrfs-default.bin.gz")


@pytest.fixture
def btrfs_sparse() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/btrfs-sparse.bin.gz")


@pytest.fixture
def btrfs_subvolume() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/btrfs-subvolume.bin.gz")


@pytest.fixture
def btrfs_subvolume_nested() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/btrfs-subvolume-nested.bin.gz")


@pytest.fixture
def btrfs_subvolume_custom_default() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/btrfs-subvolume-custom-default.bin.gz")


@pytest.fixture
def btrfs_subvolume_snapshot() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/btrfs-subvolume-snapshot.bin.gz")


@pytest.fixture
def btrfs_compression() -> Iterator[BinaryIO]:
    yield from open_file_gz("data/btrfs-compression.bin.gz")


@pytest.fixture
def btrfs_profile_dup() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/btrfs-dup-1.bin.gz", "data/btrfs-dup-2.bin.gz"])


@pytest.fixture
def btrfs_profile_raid0() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/btrfs-raid0-1.bin.gz", "data/btrfs-raid0-2.bin.gz"])


@pytest.fixture
def btrfs_profile_raid1() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/btrfs-raid1-1.bin.gz", "data/btrfs-raid1-2.bin.gz"])


@pytest.fixture
def btrfs_profile_raid1c3() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "data/btrfs-raid1c3-1.bin.gz",
            "data/btrfs-raid1c3-2.bin.gz",
            "data/btrfs-raid1c3-3.bin.gz",
        ]
    )


@pytest.fixture
def btrfs_profile_raid1c4() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(
        [
            "data/btrfs-raid1c4-1.bin.gz",
            "data/btrfs-raid1c4-2.bin.gz",
            "data/btrfs-raid1c4-3.bin.gz",
            "data/btrfs-raid1c4-4.bin.gz",
        ]
    )


@pytest.fixture
def btrfs_profile_raid5() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/btrfs-raid5-1.bin.gz", "data/btrfs-raid5-2.bin.gz"])


@pytest.fixture
def btrfs_profile_raid6() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/btrfs-raid6-1.bin.gz", "data/btrfs-raid6-2.bin.gz", "data/btrfs-raid6-3.bin.gz"])


@pytest.fixture
def btrfs_profile_raid10() -> Iterator[list[BinaryIO]]:
    yield from open_files_gz(["data/btrfs-raid10-1.bin.gz", "data/btrfs-raid10-2.bin.gz"])
