#!/usr/bin/env python3
from mavftp import MavFtpClient

import os
import argparse
import errno
import asyncio
import signal
import stat
import time
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
        self.files_by_handle = {}

        self.inode_counter = pyfuse3.ROOT_INODE + 1
        self.fh_counter = 0

    async def getattr(self, inode, ctx=None):
        entry = pyfuse3.EntryAttributes()
        if inode in self.files_by_inode:
            file = self.files_by_inode[inode]
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
        parent = self.files_by_inode.get(parent_inode, None)
        if parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        full_path = parent.path + "/" + str(name)
        # Handle 
        if name == '.':
            return await self.getattr(parent_inode)
        elif name == '..':
            return await self.getattr(parent.parent.inode)
        elif full_path not in self.files_by_path:
            # Check directory listing
            await self.mavlink_opendir(parent)
            if full_path not in self.files_by_path:
                # Still not found, raise ENOENT
                raise pyfuse3.FUSEError(errno.ENOENT)

        return await self.getattr(self.files_by_path[full_path].inode)

    async def opendir(self, inode, ctx):
        if inode not in self.files_by_inode:
            raise pyfuse3.FUSEError(errno.ENOENT)

        fh = self.fh_counter
        self.fh_counter += 1
        self.files_by_handle[fh] = self.files_by_inode[inode]
        return fh
    
    async def open(self, inode, flags, ctx):
        if inode not in self.files_by_inode:
            raise pyfuse3.FUSEError(errno.ENOENT)
        
        # Is the file already open?
        for fh, file in self.files_by_handle.items():
            if file.inode == inode:
                return pyfuse3.FileInfo(fh=fh, direct_io=True, keep_cache=False, nonseekable=False)
        
        if not await self.client.op_open_file_ro(self.files_by_inode[inode].path):
            raise pyfuse3.FUSEError(errno.EIO)

        fh = self.fh_counter
        self.fh_counter += 1
        self.files_by_handle[fh] = self.files_by_inode[inode]
        return pyfuse3.FileInfo(fh=fh, direct_io=True, keep_cache=False, nonseekable=False)
    
    async def read(self, fh, offset, length):
        if fh not in self.files_by_handle:
            raise pyfuse3.FUSEError(errno.EBADF)
        
        file = self.files_by_handle[fh]
        if file.type == 'directory':
            raise pyfuse3.FUSEError(errno.EISDIR)
        
        data = await self.client.op_read_file(file.path, offset, length)
        if data is None:
            raise pyfuse3.FUSEError(errno.EIO)
        
        return data
    
    async def release(self, fh) -> None:
        if fh in self.files_by_handle:
            await self.client.op_close_session(self.files_by_handle[fh].path)
            try:
                del self.files_by_handle[fh]
            except KeyError:
                print(f"Warn: Tried to remove non-existent file handle {fh}")
        else:
            raise pyfuse3.FUSEError(errno.EBADF)
    
    async def releasedir(self, fh):
        if fh in self.files_by_handle:
            del self.files_by_handle[fh]
        else:
            raise pyfuse3.FUSEError(errno.EBADF)
    
    async def mavlink_opendir(self, dir: File):
        files = []
        mavlink_entries = await self.client.op_list_directory(dir.path)
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
        dir = self.files_by_handle.get(fh, None)
        if dir is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

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

shutdown_event = asyncio.Event()

def sigint_handler():
    if shutdown_event.is_set():
        print("Forcing exit")
        os._exit(1)
    
    shutdown_event.set()


async def main(connection_str: str, mountpoint: str, write_mode: bool = False):
    asyncio.get_event_loop().add_signal_handler(
        signal.SIGINT,
        sigint_handler)
    client = MavFtpClient()
    try:
        await asyncio.wait_for(client.connect(connection_str), timeout=5)
    except asyncio.TimeoutError:
        print("MAVLink connection timed out")
        return

    # Mount the filesystem
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('debug')
    fuse_options.add('auto_unmount')
    fuse_options.add('ro')
    fuse_options.add('sync')
    fuse_options.add('dirsync')
    fuse_options.add('noatime')
    pyfuse3.init(MavFtpFS(client), mountpoint, fuse_options)
    asyncio.create_task(pyfuse3.main())

    # Wait for sigint
    await shutdown_event.wait()

    pyfuse3.close(unmount=True)
    await client.close()


if __name__ == '__main__':
    # Setup argparse
    parser = argparse.ArgumentParser(description='Connect to a MAVLink vehicle and specify a mountpoint.')
    
    # Positional argument for the MAVLink connection string
    parser.add_argument('connection_string', type=str, 
                        help='MAVLink connection string (e.g., udpin:localhost:14540, /dev/ttyUSB0, tcp:127.0.0.1:5760)')
    
    # Positional argument for the mountpoint
    parser.add_argument('mountpoint', type=str, 
                        help='Mountpoint path (e.g., /mnt/mavlink)')
    
    # Optional flag for write mode
    parser.add_argument('-w', '--write', action='store_true', 
                        help='Enable write mode (optional, default is read-only).')

    # Parse the arguments
    args = parser.parse_args()
    asyncio.run(main(args.connection_string, args.mountpoint, args.write))
