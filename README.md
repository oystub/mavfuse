# MAVFUSE
My attempt at creating a FUSE3 filesystem over a MAVFTP connection.
This makes it simple to transfer files to and from the SD card of a PX4 based drone without having to physically remove the SD card.

Supports basic file operations like read, write, open, close, etc.

**Note that since I started work on this, a MAVFTP implementation with experimental FUSE module has beed added to [pymavlink](https://github.com/ArduPilot/pymavlink). As of writing, it only provides read support, and is unstable on my computer. However, with time, it will likely become the better option.**

## Installation
1. Install FUSE3 system libraries (Ubuntu/Debian: `sudo apt install fuse3 libfuse3-dev`)
2. Install the required python packages (`pip install -r requirements.txt`)

## Usage
`python3 -m mavfuse <connection_string> <mountpoint> [-w] [-d] [--list_crc32_xattr]`

- `-w` enables write support
- `-d` Print debug information to the console.
- `--list_crc32_xattr` unless specified, the `user.mavlink.crc32` xattr is hidden unless directly requested. This is because the checksum is computationally expensive for the FCU to calculate, and is not always needed.
*(NB! MAVFTP uses a non-standard CRC32 algorithm. See `crc32.py`)*

E.g. `python3 -m mavfuse tcp:127.0.0.1:14550 ~/mnt/mav`
