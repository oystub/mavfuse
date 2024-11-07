#!/usr/bin/env python3
import argparse
import asyncio
import errno
import os
import signal
import stat
import sys
import time
import struct
import logging
from dataclasses import dataclass

import pyfuse3
import pyfuse3.asyncio

from mavftp import MavFtpClient, MavFtpNotEmptyError

pyfuse3.asyncio.enable()


@dataclass
class File:
    inode: int
    size: int
    path: str
    type: str
    parent: "File"


class MavFtpFS(pyfuse3.Operations):
    def __init__(self, client: MavFtpClient, list_crc32_xattr: bool = False):
        self._client = client
        self._list_crc32_xattr = list_crc32_xattr
        root_file = File(inode=pyfuse3.ROOT_INODE, path="",
                         parent=None, size=0, type='directory')
        root_file.parent = root_file  # Root directory is its own parent

        self._files_by_inode = {
            pyfuse3.ROOT_INODE: root_file
        }
        self._files_by_path = {
            '': self._files_by_inode[pyfuse3.ROOT_INODE]
        }
        self._files_by_handle = {}

        self._inode_counter = pyfuse3.ROOT_INODE + 1
        self._fh_counter = 0

    async def getattr(self, inode, ctx=None):
        entry = pyfuse3.EntryAttributes()
        if inode in self._files_by_inode:
            file = self._files_by_inode[inode]
            entry.st_mode = (stat.S_IFDIR | 0o755) if file.type == 'directory' else (
                stat.S_IFREG | 0o644)
            entry.st_size = file.size
        else:
            raise pyfuse3.FUSEError(errno.ENOENT)

        stamp = time.time_ns()
        entry.st_atime_ns = stamp
        entry.st_ctime_ns = stamp
        entry.st_mtime_ns = stamp
        entry.st_gid = os.getgid()
        entry.st_uid = os.getuid()
        entry.st_ino = inode

        return entry

    async def lookup(self, parent_inode, name, ctx=None):
        name_str = name.decode("utf-8")
        parent = self._files_by_inode.get(parent_inode, None)
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        full_path = parent.path + "/" + name_str
        # Handle "." and ".."
        if name_str == '.':
            return await self.getattr(parent_inode)
        elif name_str == '..':
            return await self.getattr(parent.parent.inode)
        elif full_path not in self._files_by_path:
            # Check directory listing
            await self.mavlink_opendir(parent)
            if full_path not in self._files_by_path:
                # Still not found, raise ENOENT
                raise pyfuse3.FUSEError(errno.ENOENT)

        return await self.getattr(self._files_by_path[full_path].inode)

    async def opendir(self, inode, ctx):
        if inode not in self._files_by_inode:
            raise pyfuse3.FUSEError(errno.ENOENT)

        fh = self._fh_counter
        self._fh_counter += 1
        self._files_by_handle[fh] = self._files_by_inode[inode]
        return fh

    async def open(self, inode, flags, ctx):
        if inode not in self._files_by_inode:
            raise pyfuse3.FUSEError(errno.ENOENT)


        # Check for O_TRUNC flag and write mode
        if flags & os.O_TRUNC and (flags & os.O_WRONLY or flags & os.O_RDWR):
            # Truncate the file to 0 bytes
            if not await self._client.truncate_file(self._files_by_inode[inode].path, 0):
                raise pyfuse3.FUSEError(errno.EIO)

        # Is the file already open?
        for fh, file in self._files_by_handle.items():
            if file.inode == inode:
                return pyfuse3.FileInfo(fh=fh, direct_io=True, keep_cache=False, nonseekable=False)

        if not await self._client.open_file_ro(self._files_by_inode[inode].path):
            raise pyfuse3.FUSEError(errno.EIO)

        fh = self._fh_counter
        self._fh_counter += 1
        self._files_by_handle[fh] = self._files_by_inode[inode]
        return pyfuse3.FileInfo(fh=fh, direct_io=True, keep_cache=False, nonseekable=False)

    async def read(self, fh, offset, length):
        if fh not in self._files_by_handle:
            raise pyfuse3.FUSEError(errno.EBADF)

        file = self._files_by_handle[fh]
        if file.type == 'directory':
            raise pyfuse3.FUSEError(errno.EISDIR)

        data = await self._client.read_file(file.path, offset, length)
        if data is None:
            raise pyfuse3.FUSEError(errno.EIO)

        return data

    async def release(self, fh) -> None:
        if fh in self._files_by_handle:
            await self._client.close_session(self._files_by_handle[fh].path)
            try:
                del self._files_by_handle[fh]
            except KeyError:
                logging.warning(f"Tried to remove non-existent file handle {fh}")
        else:
            raise pyfuse3.FUSEError(errno.EBADF)

    async def releasedir(self, fh):
        if fh in self._files_by_handle:
            del self._files_by_handle[fh]
        else:
            raise pyfuse3.FUSEError(errno.EBADF)

    async def mavlink_opendir(self, dir: File):
        files = []
        mavlink_entries = await self._client.list_directory(dir.path)
        if mavlink_entries is None:
            raise pyfuse3.FUSEError(errno.EIO)
        for entry in mavlink_entries:
            if entry['type'] == 'skip':
                continue
            # Check if the file already exists, then we can reuse the inode
            existing_file = self._files_by_path.get(
                dir.path + "/" + entry['name'], None)
            if existing_file is None:
                # If not, create a new inode
                inode = self._inode_counter
                self._inode_counter += 1
            else:
                inode = existing_file.inode

            f = File(
                inode=inode,
                parent=dir,
                path=dir.path + "/" + entry['name'],
                size=entry.get('size', 0),
                type=entry['type']
            )
            self._files_by_inode[f.inode] = f
            self._files_by_path[f.path] = f

            files.append((entry['name'], f))

        return files

    async def readdir(self, fh, start_id, token):
        dir = self._files_by_handle.get(fh, None)
        if dir is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        dir_files = [
            (".", dir),
            ("..", dir.parent)
        ]
        dir_files.extend(await self.mavlink_opendir(dir))
        # Iterate, starting at the requested ID
        for f in dir_files[start_id:]:
            res = pyfuse3.readdir_reply(token, f[0].encode("utf-8"), await self.getattr(f[1].inode), start_id + 1)
            if not res:
                break
            start_id += 1

    async def getxattr(self, inode, name, ctx):
        name_str = name.decode("utf-8")
        file = self._files_by_inode.get(inode, None)
        if file is None or file.type != 'file':
            raise pyfuse3.FUSEError(errno.ENOENT)

        if name_str != "user.mavlink.crc32":
            raise pyfuse3.FUSEError(pyfuse3.ENOATTR)

        crc = await self._client.crc32(self._files_by_inode[inode].path)
        return struct.pack("<I", crc)

    async def listxattr(self, inode, ctx):
        file = self._files_by_inode.get(inode, None)
        if file is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if file.type != 'file':
            return []
        if self._list_crc32_xattr:
            return [b"user.mavlink.crc32"]
        # By default, hide the crc32 xattr from the user when listing
        # It is computationally for the fc to calculate
        # And we want to prevent programs from requesting it by default
        # since it is often not needed.
        return []

    async def create(self, parent_inode, name, mode, flags, ctx):
        name_str = name.decode("utf-8")
        parent = self._files_by_inode.get(parent_inode, None)
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        full_path = parent.path + "/" + name_str

        # TODO: See if we need to check flags
        # TODO: Check if file already exists, currently will truncate it

        if await self._client.create_file(full_path):
            inode = self._inode_counter
            self._inode_counter += 1
            f = File(
                inode=inode,
                parent=parent,
                path=full_path,
                size=0,
                type='file'
            )
            self._files_by_inode[f.inode] = f
            self._files_by_path[f.path] = f
            return (await self.open(f.inode, flags, ctx), await self.getattr(f.inode))
        else:
            raise pyfuse3.FUSEError(errno.EIO)

    async def mkdir(self, parent_inode, name, mode, ctx):
        name_str = name.decode("utf-8")
        parent = self._files_by_inode.get(parent_inode, None)
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        full_path = parent.path + "/" + name_str

        if await self._client.create_directory(full_path):
            inode = self._inode_counter
            self._inode_counter += 1
            f = File(
                inode=inode,
                parent=parent,
                path=full_path,
                size=0,
                type='directory'
            )
            self._files_by_inode[f.inode] = f
            self._files_by_path[f.path] = f
            return await self.getattr(f.inode)
        else:
            raise pyfuse3.FUSEError(errno.EIO)

    async def unlink(self, parent_inode, name, ctx):
        name_str = name.decode("utf-8")
        parent = self._files_by_inode.get(parent_inode, None)
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        full_path = parent.path + "/" + name_str

        await self.lookup(parent_inode, name)

        file = self._files_by_path.get(full_path, None)
        if file.type != 'file':
            raise pyfuse3.FUSEError(errno.EISDIR)

        if not await self._client.remove_file(file.path):
            raise pyfuse3.FUSEError(errno.EIO)

        del self._files_by_path[full_path]
        del self._files_by_inode[file.inode]

    async def rmdir(self, parent_inode, name, ctx):
        name_str = name.decode("utf-8")
        parent = self._files_by_inode.get(parent_inode, None)
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        full_path = parent.path + "/" + name_str

        await self.lookup(parent_inode, name)

        file = self._files_by_path.get(full_path, None)
        if file.type != 'directory':
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        try:
            await self._client.remove_directory(file.path)
        except MavFtpNotEmptyError:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)
        del self._files_by_path[full_path]
        del self._files_by_inode[file.inode]

    async def write(self, fh, offset, data):
        if fh not in self._files_by_handle:
            raise pyfuse3.FUSEError(errno.EBADF)

        file = self._files_by_handle[fh]
        if file.type == 'directory':
            raise pyfuse3.FUSEError(errno.EISDIR)

        # We cannot write more than 239 bytes at a time due to MAVLink limitations
        # Since we are in direct_io mode, we are allowed to write less than the requested amount
        if len(data) > 239:
            data = data[:239]

        if not await self._client.write(file.path, offset, data):
            raise pyfuse3.FUSEError(errno.EIO)

        return len(data)

    async def rename(self, parent_inode, name, new_parent_inode, new_name, flags, ctx):
        name_str = name.decode("utf-8")
        parent = self._files_by_inode.get(parent_inode, None)
        new_parent = self._files_by_inode.get(new_parent_inode, None)
        if parent is None or new_parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        full_path = parent.path + "/" + name_str
        new_full_path = new_parent.path + "/" + new_name.decode("utf-8")

        await self.lookup(parent_inode, name)

        file = self._files_by_path.get(full_path, None)
        if file is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        await self._client.rename(file.path, new_full_path)
        file.path = new_full_path
        self._files_by_path[new_full_path] = file
        del self._files_by_path[full_path]

    async def truncate(self, inode, length, ctx):
        file = self._files_by_inode.get(inode, None)
        if file is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if file.type != 'file':
            raise pyfuse3.FUSEError(errno.EISDIR)

        if not await self._client.truncate_file(file.path, length):
            raise pyfuse3.FUSEError(errno.EIO)

        file.size = length


shutdown_event = asyncio.Event()


def sigint_handler():
    if shutdown_event.is_set():
        logging.info("Forcing exit")
        os._exit(1)

    shutdown_event.set()


async def main(connection_str: str, mountpoint: str, write_mode: bool = False, debug: bool = False, list_crc32_xattr: bool = False):
    # Set up logging
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    asyncio.get_event_loop().add_signal_handler(
        signal.SIGINT,
        sigint_handler)
    client = MavFtpClient()
    try:
        await asyncio.wait_for(client.connect(connection_str), timeout=5)
    except asyncio.TimeoutError:
        logging.error("MAVLink connection timed out")
        return

    # Mount the filesystem
    fuse_options = set(pyfuse3.default_options)
    if debug:
        fuse_options.add('debug')
        root_logger.setLevel(logging.DEBUG)
    fuse_options.add('auto_unmount')
    fuse_options.add('sync')
    fuse_options.add('dirsync')
    fuse_options.add('noatime')
    if write_mode:
        fuse_options.add('rw')
    else:
        fuse_options.add('ro')
    pyfuse3.init(MavFtpFS(client, list_crc32_xattr), mountpoint, fuse_options)
    asyncio.create_task(pyfuse3.main())

    try:
        await shutdown_event.wait()
    finally:
        await client.close()
        pyfuse3.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Connect to a MAVLink vehicle and specify a mountpoint.')

    parser.add_argument('connection_string', type=str,
                        help='MAVLink connection string (e.g., udpin:localhost:14540, /dev/ttyUSB0, tcp:127.0.0.1:5760)')
    parser.add_argument('mountpoint', type=str,
                        help='Mountpoint path (e.g., /mnt/mavlink)')
    parser.add_argument('-w', '--write', action='store_true',
                        help='Enable write mode (optional, default is read-only).')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug mode (optional).')
    parser.add_argument('--list_crc32_xattr', action='store_true',
                        help="By default, the CRC32 xattr is hidden when listing xattrs, since the value is computationally expensive for the FC to calculate. This flag will show it in the xattr list.")

    args = parser.parse_args()
    asyncio.run(main(args.connection_string, args.mountpoint, args.write, args.debug, args.list_crc32_xattr))
