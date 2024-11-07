import zlib


def compute_file_crc32(file):
    # Note that the MAVLink CRC32 checksum is different from the zlib CRC32 checksum.
    # https://mavlink.io/en/guide/crc.html
    # Therefore, we start with a checksum of 0xFFFFFFFF, and invert the final checksum.

    checksum = 0xFFFFFFFF
    buffer_size = 65536  # Read in chunks of 64KB

    while True:
        data = file.read(buffer_size)
        if not data:
            break
        checksum = zlib.crc32(data, checksum)

    # Ensure checksum is in unsigned 32-bit format
    return checksum ^ 0xFFFFFFFF


if __name__ == "__main__":
    # Get filename from argv
    import sys
    if len(sys.argv) != 2:
        print("Usage: crc32.py <filename>")
        sys.exit(1)
    filename = sys.argv[1]

    try:
        f = open(filename, "rb")
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        sys.exit(1)
    except IsADirectoryError:
        print(f"Error: '{filename}' is a directory")
        sys.exit(1)
    except Exception:
        print(f"Error: Could not open file '{filename}'")
        sys.exit(1)

    crc32_checksum = compute_file_crc32(f)
    print(f"CRC32 Checksum: {crc32_checksum}")