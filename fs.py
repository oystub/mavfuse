#!/usr/bin/env python3
from mavftp import MavFtpClient

import os
import sys
import errno
import asyncio
import signal
import stat
from dataclasses import dataclass
import pyfuse3
import pyfuse3.asyncio
pyfuse3.asyncio.enable()


@dataclass
class File:
    inode: int
    size: int
    path: str
    type: str
    parent: "File"


class MavFtpFS(pyfuse3.Operations):
    def __init__(self, client: MavFtpClient):
        self.client = client
        root_file = File(inode=pyfuse3.ROOT_INODE, path="",
                         parent=None, size=0, type='directory')
        root_file.parent = root_file  # Root directory is its own parent

        self.files_by_inode = {
            pyfuse3.ROOT_INODE: root_file
        }
        self.files_by_path = {
            '': self.files_by_inode[pyfuse3.ROOT_INODE]
        }

        self.inode_counter = pyfuse3.ROOT_INODE + 1

    async def getattr(self, inode, ctx=None):
        entry = pyfuse3.EntryAttributes()
        if inode in self.files_by_inode:
            file = self.files_by_inode[inode]
            entry.st_mode = (stat.S_IFDIR | 0o755) if file.type == 'directory' else (
                stat.S_IFREG | 0o644)
            entry.st_size = file.size
        else:
            raise pyfuse3.FUSEError(errno.ENOENT)

        stamp = int(1438467123.985654 * 1e9)
        entry.st_atime_ns = stamp
        entry.st_ctime_ns = stamp
        entry.st_mtime_ns = stamp
        entry.st_gid = os.getgid()
        entry.st_uid = os.getuid()
        entry.st_ino = inode

        return entry

    async def lookup(self, parent_inode, name, ctx=None):
        parent = self.files_by_inode.get(parent_inode, None)
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        full_path = parent.path + "/" + str(name)
        if full_path not in self.files_by_path:
            # Check directory listing
            await self.mavlink_opendir(parent)
            if full_path not in self.files_by_path:
                # Still not found, raise ENOENT
                raise pyfuse3.FUSEError(errno.ENOENT)

        return await self.getattr(self.files_by_path[full_path].inode)

    async def opendir(self, inode, ctx):
        return inode

    async def mavlink_opendir(self, dir: File):
        files = []
        mavlink_entries = await self.client.list_directory(dir.path)
        for entry in mavlink_entries:
            if entry['type'] == 'skip':
                continue
            # Check if the file already exists, then we can reuse the inode
            existing_file = self.files_by_path.get(
                dir.path + "/" + entry['name'], None)
            if existing_file is None:
                # If not, create a new inode
                inode = self.inode_counter
                self.inode_counter += 1
            else:
                inode = existing_file.inode

            f = File(
                inode=inode,
                parent=dir,
                path=dir.path + "/" + str(entry['name']),
                size=entry.get('size', 0),
                type=entry['type']
            )
            self.files_by_inode[f.inode] = f
            self.files_by_path[f.path] = f

            files.append((str(entry['name']), f))

        return files

    async def readdir(self, fh, start_id, token):
        dir = self.files_by_inode[fh]

        dir_files = [
            (".", dir),
            ("..", dir.parent)
        ]
        dir_files.extend(await self.mavlink_opendir(dir))
        # Iterate, starting at the requested ID
        for f in dir_files[start_id:]:
            res = pyfuse3.readdir_reply(token, f[0].encode("utf-8"), await self.getattr(f[1].inode), start_id+1)
            if not res:
                break
            start_id += 1


async def main(mountpoint):
    shutdown_event = asyncio.Event()
    asyncio.get_event_loop().add_signal_handler(
        signal.SIGINT,
        lambda: shutdown_event.set())
    client = MavFtpClient()
    await client.connect("/dev/ttyACM1")

    # Mount the filesystem
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('debug')
    fuse_options.add('ro')
    fuse_options.add('fsname=mavftp')
    pyfuse3.init(MavFtpFS(client), mountpoint, fuse_options)
    asyncio.create_task(pyfuse3.main())

    # Wait for sigint
    await shutdown_event.wait()

    pyfuse3.close(unmount=True)
    await client.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: {} <mountpoint>'.format(sys.argv[0]))
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
