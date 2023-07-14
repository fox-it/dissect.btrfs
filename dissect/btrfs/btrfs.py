# References:
# - https://btrfs.readthedocs.io/en/latest/
# - https://github.com/torvalds/linux/tree/master/fs/btrfs
# - https://github.com/btrfs/btrfs-dev-docs
# - https://www.sciencedirect.com/science/article/pii/S1742287618301993
from __future__ import annotations

import io
import stat
from datetime import datetime
from functools import cache, cached_property, lru_cache
from typing import BinaryIO, Iterator, Optional, Union
from uuid import UUID

from dissect.util import ts
from dissect.util.stream import BufferedStream

from dissect.btrfs.c_btrfs import FT_MAP, c_btrfs
from dissect.btrfs.exceptions import NotADirectoryError, NotASymlinkError
from dissect.btrfs.stream import ChunkStream, Extent, ExtentStream, decode_extent
from dissect.btrfs.tree import BTree


class Btrfs:
    """Btrfs filesystem implementation.

    This implementation supports most basic Btrfs features such as subvolumes, compression and (meta)data RAID.
    To open a RAID volume, simply pass all file-like objects that belong to the RAID set as a list.

    Args:
        fh: A file-like object for the volume to use for parsing Btrfs.
    """

    def __init__(self, fh: Union[BinaryIO, list[BinaryIO]]):
        self.fhs = fh if isinstance(fh, list) else [fh]

        if not self.fhs:
            raise ValueError("At least one file-like object is required")

        sb_fhs = []
        for fh in self.fhs:
            fh.seek(c_btrfs.BTRFS_SUPER_INFO_OFFSET)
            sb = c_btrfs.btrfs_super_block(fh)
            if sb.magic != c_btrfs.BTRFS_MAGIC:
                raise ValueError("Invalid btrfs superblock")
            sb_fhs.append((sb, fh))

        if len({sb.fsid for sb, _ in sb_fhs}) > 1:
            raise ValueError("Only file-like objects of the same filesystem UUID can be used")

        sb_fhs = sorted(sb_fhs, key=lambda item: item[0].generation, reverse=True)

        self.sb = sb_fhs[0][0]
        self.sector_size = self.sb.sectorsize
        self.node_size = self.sb.nodesize
        self.stripe_size = self.sb.stripesize

        self.label = self.sb.label.split(b"\x00")[0].decode()
        self.uuid = UUID(bytes=self.sb.fsid)
        self.metadata_uuid = UUID(bytes=self.sb.metadata_uuid)

        self.devices = {sb.dev_item.devid: fh for sb, fh in sb_fhs}

        self._open_tree = cache(self._open_tree)
        self._open_volume = cache(self._open_volume)

        self._logical_fh = ChunkStream(self)
        self._initialize_chunks()

        self._root_tree = BTree(self, root_offset=self.sb.root)

        self.fs_tree = self._open_volume(c_btrfs.BTRFS_FS_TREE_OBJECTID, "<FS_TREE>")

        default_volume = self._open_default_volume()
        self.root = default_volume.root

    def get(self, path: Union[str, int], node: Optional[INode] = None) -> INode:
        """Retrieve a Btrfs inode by path or inode number.

        Args:
            path: Filesystem path or inode number.
            node: Optional inode used for relative lookups.
        """
        if isinstance(path, int):
            return self.root.volume.inode(path)

        node = node or self.root

        parts = path.split("/")

        for part in parts:
            if not part:
                continue

            if part == ".":
                continue

            if part == "..":
                node = node.parent or node
                continue

            while node.is_symlink():
                node = node.link_inode

            for name, entry in node.iterdir():
                if name == part:
                    node = entry
                    break
            else:
                raise FileNotFoundError(f"File not found: {path}")

        return node

    def _initialize_chunks(self) -> None:
        """Initialize the logical data stream by reading the system chunk array and traversing the chunk tree."""
        sys_chunk_array = io.BytesIO(memoryview(self.sb.sys_chunk_array)[: self.sb.sys_chunk_array_size])
        while True:
            key = c_btrfs.btrfs_disk_key(sys_chunk_array)
            if key.type != c_btrfs.BTRFS_CHUNK_ITEM_KEY:
                raise ValueError(f"Invalid item type in sys_chunk_array: {key}")

            chunk = c_btrfs.btrfs_chunk(sys_chunk_array)
            self._logical_fh.add(key.offset, chunk)

            if sys_chunk_array.tell() == self.sb.sys_chunk_array_size:
                break

        chunk_tree = BTree(self, root_offset=self.sb.chunk_root)
        for item, data in chunk_tree.cursor().iter(
            objectid=c_btrfs.BTRFS_FIRST_CHUNK_TREE_OBJECTID, type=c_btrfs.BTRFS_CHUNK_ITEM_KEY
        ):
            chunk = c_btrfs.btrfs_chunk(data)
            self._logical_fh.add(item.key.offset, chunk)

    def _open_tree(self, objectid: int) -> BTree:
        """Open a tree by object ID.

        Args:
            objectid: The object ID of the tree to open.
        """
        _, data = self._root_tree.search(objectid, c_btrfs.BTRFS_ROOT_ITEM_KEY)
        root_item = c_btrfs.btrfs_root_item(data)
        return BTree(self, root_item)

    def _open_default_volume(self) -> Volume:
        """Find and open the volume that's configured as default."""
        _, data = self._root_tree.search(c_btrfs.BTRFS_ROOT_TREE_DIR_OBJECTID, c_btrfs.BTRFS_DIR_ITEM_KEY)
        root_dir_item = c_btrfs.btrfs_dir_item(data)
        return Volume(self, root_dir_item.location.objectid, root_dir_item.name.decode(errors="surrogateescape"))

    def _open_volume(self, objectid: int, name: str, parent: Optional[INode] = None) -> Volume:
        """Helper method for opening a volume."""
        return Volume(self, objectid, name, parent)

    def _read_node(self, address: int) -> bytes:
        """Helper method for reading a node."""
        self._logical_fh.seek(address)
        return self._logical_fh.read(self.node_size)


class Volume:
    """Represent a Btrfs volume.

    Btrfs has support for multiple volumes. The default volume is the ``FS_TREE`` volume.
    Each (sub)volume has its own B-tree.

    Args:
        btrfs: The filesystem this volume belongs to.
        objectid: The object ID of the volume to open.
        name: The name of the volume.
        parent: Optional parent node to attach to the root of the volume.
    """

    def __init__(self, btrfs: Btrfs, objectid: int, name: str, parent: Optional[INode] = None):
        self.btrfs = btrfs
        self.objectid = objectid
        self.name = name

        self._tree = self.btrfs._open_tree(self.objectid)
        self.uuid = UUID(bytes=self._tree.root_item.uuid)

        self.inode = lru_cache(8192)(self.inode)

        self.root = self.inode(self._tree.root_item.root_dirid, c_btrfs.BTRFS_FT_DIR, parent)

    def __repr__(self) -> str:
        return f"<Volume objectid={self.objectid} name={self.name!r}>"

    def inode(self, inum: int, type: Optional[int] = None, parent: Optional[INode] = None) -> INode:
        """Return an :class:`INode` by number, optionally attaching a type and parent."""
        return INode(self, inum, type, parent)


class INode:
    """Represent a Btrfs inode.

    Args:
        volume: The volume this inode belongs to.
        inum: The inode number of this inode.
        type: Optional file type of this inode, as observed in a directory entry.
        parent: Optional parent of this inode, if this inode is parsed from a directory listing.
    """

    def __init__(
        self,
        volume: Volume,
        inum: int,
        type: Optional[int] = None,
        parent: Optional[INode] = None,
    ):
        self.volume = volume
        self.btrfs = volume.btrfs
        self.inum = inum
        self._type = type
        self.parent = parent

        self.listdir = cache(self.listdir)

    def __repr__(self) -> str:
        return f"<inode {self.volume.objectid}:{self.inum}>"

    @cached_property
    def inode(self) -> c_btrfs.btrfs_inode_item:
        """Return the parsed inode structure."""
        _, data = self.volume._tree.search(self.inum, c_btrfs.BTRFS_INODE_ITEM_KEY)
        return c_btrfs.btrfs_inode_item(data)

    @cached_property
    def size(self) -> int:
        """Return the file size."""
        return self.inode.size

    @cached_property
    def uid(self) -> int:
        """Return the owner user ID."""
        return self.inode.uid

    @cached_property
    def gid(self) -> int:
        """Return the owner group ID."""
        return self.inode.gid

    @cached_property
    def mode(self) -> int:
        """Return the file mode."""
        return self.inode.mode

    @cached_property
    def type(self) -> int:
        """Return the file type."""
        return FT_MAP[self._type] or stat.S_IFMT(self.inode.mode)

    @cached_property
    def atime(self) -> datetime:
        """Return datetime timestamp of last access."""
        return ts.from_unix_ns(self.atime_ns)

    @cached_property
    def atime_ns(self) -> int:
        """Return nanosecond timestamp of last access."""
        return _parse_ts(self.inode.atime)

    @cached_property
    def ctime(self) -> datetime:
        """Return datetime timestamp of last metadata change."""
        return ts.from_unix_ns(self.ctime_ns)

    @cached_property
    def ctime_ns(self) -> int:
        """Return nanosecond timestamp of last metadata change."""
        return _parse_ts(self.inode.ctime)

    @cached_property
    def mtime(self) -> datetime:
        """Return datetime timestamp of last content modification."""
        return ts.from_unix_ns(self.mtime_ns)

    @cached_property
    def mtime_ns(self) -> int:
        """Return nanosecond timestamp of last content modification."""
        return _parse_ts(self.inode.mtime)

    @cached_property
    def otime(self) -> datetime:
        """Return datetime timestamp of inode creation."""
        return ts.from_unix_ns(self.otime_ns)

    @cached_property
    def otime_ns(self) -> int:
        """Return nanosecond timestamp of inode creation."""
        return _parse_ts(self.inode.otime)

    def is_dir(self) -> bool:
        """Return whether this inode is a directory."""
        return self.type == stat.S_IFDIR

    def is_file(self) -> bool:
        """Return whether this inode is a regular file."""
        return self.type == stat.S_IFREG

    def is_symlink(self) -> bool:
        """Return whether this inode is a symlink."""
        return self.type == stat.S_IFLNK

    def is_block_device(self) -> bool:
        """Return whether this inode is a block device."""
        return self.type == stat.S_IFBLK

    def is_character_device(self) -> bool:
        """Return whether this inode is a character device."""
        return self.type == stat.S_IFCHR

    def is_device(self) -> bool:
        """Return whether this inode is a device."""
        return self.is_block_device() or self.is_character_device()

    def is_fifo(self) -> bool:
        """Return whether this inode is a FIFO file."""
        return self.type == stat.S_IFIFO

    def is_socket(self) -> bool:
        """Return whether this inode is a socket file."""
        return self.type == stat.S_IFSOCK

    def is_ipc(self) -> bool:
        """Return whether this inode is an IPC file."""
        return self.is_fifo() or self.is_socket()

    @cached_property
    def link(self) -> str:
        """Return the symlink target."""
        if not self.is_symlink():
            raise NotASymlinkError(f"{self!r} is not a symlink")

        return self.open().read().decode(errors="surrogateescape")

    @cached_property
    def link_inode(self) -> INode:
        """Resolve the symlink target to an inode."""
        link = self.link
        if link.startswith("/"):
            relnode = None
        else:
            relnode = self.parent
        return self.btrfs.get(self.link, relnode)

    @cached_property
    def parents(self) -> list[INode]:
        for item, data in self.volume._tree.cursor().iter(self.inum, c_btrfs.BTRFS_INODE_REF_KEY):
            inode_ref = c_btrfs.btrfs_inode_ref(data)
            if inode_ref.name == b"..":
                if self.parent:
                    yield self.parent
            else:
                yield self.volume.inode(item.key.offset, c_btrfs.BTRFS_FT_DIR)

    def listdir(self) -> dict[str, INode]:
        """Return a directory listing."""
        return {name: inode for name, inode in self.iterdir()}

    def iterdir(self) -> Iterator[INode]:
        """Iterate directory contents."""
        if not self.is_dir():
            raise NotADirectoryError(f"{self!r} is not a directory")

        yield ".", self
        yield "..", self.parent or self

        cursor = self.volume._tree.cursor()
        for _, data in cursor.iter(self.inum, c_btrfs.BTRFS_DIR_INDEX_KEY):
            dir_item = c_btrfs.btrfs_dir_item(data)
            dir_item_type = dir_item.location.type
            name = dir_item.name.decode(errors="surrogateescape")

            if dir_item_type == c_btrfs.BTRFS_ROOT_ITEM_KEY:
                subvolume = self.btrfs._open_volume(dir_item.location.objectid, name, self)
                yield name, subvolume.root
            elif dir_item_type == c_btrfs.BTRFS_INODE_ITEM_KEY:
                yield name, self.volume.inode(dir_item.location.objectid, dir_item.type, self)
            else:
                raise NotImplementedError(f"Unknown dir_item type: {dir_item}")

    def open(self) -> BinaryIO:
        """Return the data stream for the inode.

        File data in Btrfs can be inlined in the B-tree or stored in file extents. In both cases it can be compressed.
        """
        if not self.inode.nbytes:
            return BufferedStream(io.BytesIO(), size=0)

        extents = []

        cursor = self.volume._tree.cursor()
        for _, data in cursor.iter(self.inum, c_btrfs.BTRFS_EXTENT_DATA_KEY, 0, ignore_offset=True):
            extent = c_btrfs.btrfs_file_extent_item_inline(data)

            if extent.type == c_btrfs.BTRFS_FILE_EXTENT_INLINE:
                header_len = len(c_btrfs.btrfs_file_extent_item_inline)
                buf = decode_extent(
                    data[header_len : header_len + self.size],
                    extent.compression,
                    extent.encryption,
                    self.btrfs.sector_size,
                )
                return BufferedStream(io.BytesIO(buf), size=self.size)

            extent = c_btrfs.btrfs_file_extent_item_reg(data)
            if extent.type == c_btrfs.BTRFS_FILE_EXTENT_REG:
                extents.append(
                    Extent(
                        extent.compression,
                        extent.encryption,
                        extent.disk_bytenr,
                        extent.disk_num_bytes,
                        extent.offset,
                        extent.num_bytes,
                    )
                )

        return ExtentStream(self.btrfs._logical_fh, extents, self.size, self.btrfs.sector_size)


def _parse_ts(timespec: c_btrfs.btrfs_timespec) -> int:
    """Parse a Btrfs time specification into a nanosecond timestamp."""
    return (timespec.sec * 1000000000) + timespec.nsec
