import datetime
from typing import BinaryIO

import pytest
from dissect.util.stream import BufferedStream

from dissect.btrfs.btrfs import Btrfs
from dissect.btrfs.c_btrfs import c_btrfs
from dissect.btrfs.exceptions import Error


def assert_test_data(fs: Btrfs) -> None:
    entry = fs.get("")
    assert entry.is_dir()
    assert entry.path == ""

    entry = fs.get("path")
    assert entry.is_dir()
    assert not entry.is_file()
    assert not entry.is_symlink()
    assert not entry.is_device()
    assert not entry.is_ipc()
    assert entry.size == 4
    assert entry.path == "path"
    assert entry.uid == 1000
    assert entry.gid == 1000

    entry = fs.get("path/to/a/file.txt")
    assert entry.is_file()
    assert entry.size == 12
    assert entry.path == "path/to/a/file.txt"
    assert entry.open().read() == b"file in dir\n"

    entry = fs.get("small.txt")
    assert entry.is_file()
    assert entry.size == 29
    assert entry.path == "small.txt"
    assert entry.open().read() == b"small file content goes here\n"

    entry = fs.get("large.txt")
    assert entry.is_file()
    assert entry.size == 5242881
    assert entry.path == "large.txt"
    assert entry.open().read() == (b"a" * 5242880) + b"\n"

    entry = fs.get("link.txt")
    assert entry.is_symlink()
    assert entry.path == "link.txt"
    assert entry.link == "path/to/a/file.txt"


def test_btrfs(btrfs_default: BinaryIO) -> None:
    fs = Btrfs(btrfs_default)

    assert fs.label == "btrfs-default"
    assert str(fs.uuid) == "74387226-fa97-4f42-a276-9bb07ce5e62d"

    root = fs.get("/")
    assert list(root.listdir().keys()) == [".", "..", "path", "link.txt", "small.txt", "large.txt"]

    assert_test_data(fs)

    entry = fs.get("path")
    assert entry.mode == 0o40755
    assert entry.atime == datetime.datetime(2023, 6, 28, 3, 4, 16, tzinfo=datetime.timezone.utc)
    assert entry.ctime == datetime.datetime(2023, 6, 28, 3, 4, 12, tzinfo=datetime.timezone.utc)
    assert entry.mtime == datetime.datetime(2023, 6, 28, 3, 4, 12, tzinfo=datetime.timezone.utc)
    assert entry.otime == datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)


def test_btrfs_subvolume(btrfs_subvolume: BinaryIO) -> None:
    fs = Btrfs(btrfs_subvolume)

    assert fs.label == "btrfs-subvolume"
    assert str(fs.uuid) == "74387226-fa97-4f42-a276-9bb07ce5e62d"

    subvol = fs.get("subvol")
    assert subvol.path == ""
    assert subvol.subvolume.path == "subvol"
    assert subvol.full_path == "subvol"
    assert list(subvol.listdir().keys()) == [".", "..", "cross-volume-link.txt", "small.txt", "large.txt", "some"]

    assert_test_data(fs)

    entry = fs.get("subvol/some/more/dirs/empty.txt")
    assert entry.is_file()
    assert entry.size == 0
    assert entry.path == "some/more/dirs/empty.txt"
    assert entry.full_path == "subvol/some/more/dirs/empty.txt"
    assert entry.open().read() == b""

    entry = fs.get("subvol/small.txt")
    assert entry.is_file()
    assert entry.size == 18
    assert entry.open().read() == b"file in subvolume\n"

    entry = fs.get("subvol/large.txt")
    assert entry.is_file()
    assert entry.size == 5242881
    assert entry.open().read() == (b"b" * 5242880) + b"\n"

    entry = fs.get("subvol/cross-volume-link.txt")
    assert entry.is_symlink()
    assert entry.link == "../link.txt"


def test_btrfs_subvolume_custom_default(btrfs_subvolume_custom_default: BinaryIO) -> None:
    fs = Btrfs(btrfs_subvolume_custom_default)

    assert fs.label == "btrfs-subvolume-custom-default"
    assert str(fs.uuid) == "74387226-fa97-4f42-a276-9bb07ce5e62d"


def test_btrfs_subvolume_nested(btrfs_subvolume_nested: BinaryIO) -> None:
    fs = Btrfs(btrfs_subvolume_nested)

    assert fs.label == "btrfs-subvolume-nested"
    assert str(fs.uuid) == "983bda53-2928-4182-a53d-041eade66473"

    subvolumes = list(fs.subvolumes())

    assert len(subvolumes) == 4
    assert sorted((subvol.objectid, subvol.path) for subvol in subvolumes) == [
        (5, ""),
        (256, "dir/volume"),
        (257, "default"),
        (258, "default/volume"),
    ]

    assert fs.find_subvolume("default/volume").objectid == 258


def test_btrfs_subvolume_snapshot(btrfs_subvolume_snapshot: BinaryIO) -> None:
    fs = Btrfs(btrfs_subvolume_snapshot)

    assert fs.label == "btrfs-subvolume-snapshot"
    assert str(fs.uuid) == "74387226-fa97-4f42-a276-9bb07ce5e62d"

    assert_test_data(fs)


def test_btrfs_compression(btrfs_compression: BinaryIO) -> None:
    fs = Btrfs(btrfs_compression)

    fh = fs.get("zlib.txt").open()
    assert len([extent for extent in fh.extents if extent.compression == c_btrfs.BTRFS_COMPRESS_ZLIB]) == 160
    assert fh.read() == (b"zlib" * 1024 * 1024 * 5) + b"\n"

    fh = fs.get("zlib_inline.txt").open()
    assert isinstance(fh, BufferedStream)
    assert fh.read() == (b"zlib" * 256) + b"\n"

    fh = fs.get("lzo.txt").open()
    assert len([extent for extent in fh.extents if extent.compression == c_btrfs.BTRFS_COMPRESS_LZO]) == 120
    assert fh.read() == b"lzo" * 1024 * 1024 * 5 + b"\n"

    fh = fs.get("lzo_inline.txt").open()
    assert isinstance(fh, BufferedStream)
    assert fh.read() == (b"lzo" * 256) + b"\n"

    fh = fs.get("zstd.txt").open()
    assert len([extent for extent in fh.extents if extent.compression == c_btrfs.BTRFS_COMPRESS_ZSTD]) == 160
    assert fh.read() == (b"zstd" * 1024 * 1024 * 5) + b"\n"

    fh = fs.get("zstd_inline.txt").open()
    assert isinstance(fh, BufferedStream)
    assert fh.read() == (b"zstd" * 256) + b"\n"


@pytest.mark.parametrize(
    "fixture",
    [
        "btrfs_profile_dup",
        "btrfs_profile_raid0",
        "btrfs_profile_raid1",
        "btrfs_profile_raid1c3",
        "btrfs_profile_raid1c4",
        "btrfs_profile_raid5",
        "btrfs_profile_raid6",
        "btrfs_profile_raid10",
    ],
)
def test_btrfs_profiles(fixture: str, request: pytest.FixtureRequest) -> None:
    fs = Btrfs(request.getfixturevalue(fixture))
    assert_test_data(fs)


@pytest.mark.parametrize(
    "fixture",
    [
        "btrfs_profile_dup",
        "btrfs_profile_raid0",
        "btrfs_profile_raid1",
        "btrfs_profile_raid1c3",
        "btrfs_profile_raid1c4",
        "btrfs_profile_raid5",
        "btrfs_profile_raid6",
        "btrfs_profile_raid10",
    ],
)
def test_btrfs_profiles_partial(fixture: str, request: pytest.FixtureRequest) -> None:
    fhs = request.getfixturevalue(fixture)
    if fixture == "btrfs_profile_dup":
        # This test data just so happens to have all chunks on the second device
        fhs = fhs[:-1]
    else:
        fhs = fhs[1:]

    if fixture in ("btrfs_profile_dup", "btrfs_profile_raid0"):
        with pytest.raises(Error, match="Missing stripe disk for chunk offset .+"):
            fs = Btrfs(fhs)

    elif fixture in ("btrfs_profile_raid5", "btrfs_profile_raid6"):
        pass

    else:
        fs = Btrfs(fhs)
        assert_test_data(fs)


def test_btrfs_sparse(btrfs_sparse: BinaryIO) -> None:
    fs = Btrfs(btrfs_sparse)

    entry = fs.get("sparse_start")
    assert entry.size == 0x3C000
    assert entry.extents() == [
        (0, 0, 0, 0, 0, 40 * 4096),
        (0, 0, 0xD28000, 20 * 4096, 0, 20 * 4096),
    ]
    assert entry.open().read() == ((b"\x00" * 4096) * 40) + ((b"\x01" * 4096) * 20)

    entry = fs.get("sparse_hole")
    assert entry.size == 0x3C000
    assert entry.extents() == [
        (0, 0, 0xD00000, 20 * 4096, 0, 20 * 4096),
        (0, 0, 0, 0, 0, 20 * 4096),
        (0, 0, 0xD14000, 20 * 4096, 0, 20 * 4096),
    ]
    assert entry.open().read() == ((b"\x01" * 4096) * 20) + ((b"\x00" * 4096) * 20) + ((b"\x01" * 4096) * 20)

    entry = fs.get("sparse_end")
    assert entry.size == 0x3C000
    assert entry.extents() == [
        (0, 0, 0xD3C000, 20 * 4096, 0, 20 * 4096),
        (0, 0, 0, 0, 0, 40 * 4096),
    ]
    assert entry.open().read() == ((b"\x01" * 4096) * 20) + ((b"\x00" * 4096) * 40)

    entry = fs.get("sparse_all")
    assert entry.size == 0x500000
    assert entry.extents() == [
        (0, 0, 0, 0, 0, 1280 * 4096),
    ]
    assert entry.open().read() == (b"\x00" * 4096) * 1280

    entry = fs.get("snapshot/sparse_hole")
    assert entry.size == 0x3C000
    assert entry.extents() == [
        (0, 0, 0xD00000, 20 * 4096, 0, 10 * 4096),
        (0, 0, 0xD50000, 1 * 4096, 0, 1 * 4096),
        (0, 0, 0, 0, 0, 39 * 4096),
        (0, 0, 0xD52000, 10 * 4096, 0, 10 * 4096),
    ]
    assert entry.open().read() == ((b"\x01" * 4096) * 10) + ((b"\x00" * 4096) * 50)

    entry = fs.get("snapshot/sparse_end")
    assert entry.size == 0x3C000
    assert entry.extents() == [
        (0, 0, 0xD3C000, 20 * 4096, 0, 1 * 4096),
        (0, 0, 0xD5C000, 1 * 4096, 0, 1 * 4096),
        (0, 0, 0xD3C000, 20 * 4096, 2 * 4096, 18 * 4096),
        (0, 0, 0, 0, 0, 40 * 4096),
    ]
    assert entry.open().read() == (b"\x01" * (4096 + 123)) + (b"\x02") + (b"\x01" * ((4096 * 19) - 124)) + (
        (b"\x00" * 4096) * 40
    )
