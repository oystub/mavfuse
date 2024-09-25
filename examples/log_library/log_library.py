from dataclasses import dataclass
import os
from typing import List, Dict
import shutil
import hashlib
from examples.log_library.crc32 import compute_file_crc32
from typing import Optional


@dataclass
class LogSummary:
    fc_path: str
    size: int

    def storage_name(self) -> str:
        # Create a stable hash using SHA256
        hash_object = hashlib.sha256(self.fc_path.encode("utf-8"))
        return hash_object.hexdigest()[:16] + ".ulg"
  

@dataclass
class DownloadedLog:
    fc_path: str
    size: int
    crc32: int


class LogServer:
    """
    The LogServer class is used to interact with logs on a flight controller.

    It is expected to be used in the following way:
    1. List all logs available on the vehicle.
    2. Decide if the log is relevant based on name and size.
        A. (optional) If unable to decide, use `check_information` and `check_armed`.
        to get more information without downloading the log.
    3. Download the log if it is relevant.
        A. Note that the download can be stopped and resumed from the last offset.
    4. (optional) Delete the log from the vehicle when it has been downloaded, or if it isn't relevant.
    """

    RESUME_OVERLAP_CHECK_SIZE = 1024
    RESUME_REQUIRED_OVERLAP = 512
    DOWNLOAD_BLOCK_SIZE = 65536
    DOWNLOAD_RETRIES = 3

    def __init__(self, fc_path: str):
        self._fc_path = fc_path

    def list_logs(self) -> List[LogSummary]:
        """
        Return a list of all logs available on the vehicle.
        """
        out = []

        for dirpath, dirnames, filenames in os.walk(self._fc_path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                relative_path = full_path[len(self._fc_path):].strip("/")
                out.append(LogSummary(relative_path, os.path.getsize(full_path)))
        
        return out

    def check_information(self, log: LogSummary) -> Dict[str, bytes]:
        """
        Get the information messages from the log as a key-value dictionary.
        This can be used to avoid downloading logs that are not relevant.
        """
        raise NotImplementedError

    def check_armed(self, log: LogSummary) -> bool:
        """
        Check if the vehicle was armed during the log.
        Can be used to avoid downloading logs that are not relevant.
        """
        raise NotImplementedError

    def check_crc32(self, log: LogSummary) -> int:
        """
        Have the vehicle compute the CRC32 checksum of the log. This can be
        used to verify the integrity of transferred logs.
        Note: This is not the same as the zlib CRC32 checksum.
        Note2: This operation is slow for large logs.
        """
        fc_crc32 = os.getxattr(os.path.join(self._fc_path, log.fc_path.strip("/")), 'user.mavlink.crc32')
        return int.from_bytes(fc_crc32, byteorder='little')
        

    def download_log(self, log: LogSummary, dest_filepath: str, resume: bool = False) -> Optional[DownloadedLog]:
        """
        Download a log from the vehicle to a destination path.
        The `resume` parameter can be used to resume a partially downloaded log.
        """
        # Check if the destination path already exists
        if os.path.exists(dest_filepath):
            # If it is a directory, raise an error
            if os.path.isdir(dest_filepath):
                raise ValueError("Destination filepath is a directory")
            # Open the file for writing in append mode
            try:
                dest_file = open(dest_filepath, "r+b")
            except Exception:
                raise ValueError("Unable to open destination file")
            
            if resume:
                # Get the size of the file
                dest_file.seek(0, os.SEEK_END)
                file_size = dest_file.tell()

                if file_size > log.size:
                    print("WARN: Destination file is larger than the vehicle log, will re-download")
                    resume = False
                elif file_size < self.RESUME_OVERLAP_CHECK_SIZE:
                    # No point in resuming, the file is too small
                    resume = False
                else:
                    print("Resuming existing file")
                    dest_file.seek(-self.RESUME_OVERLAP_CHECK_SIZE, os.SEEK_END)
                    last_bytes = dest_file.read(self.RESUME_OVERLAP_CHECK_SIZE)
        else:
            # Create the file
            dest_file = open(dest_filepath, "w+b")
            resume = False

        print(f"Downloading {os.path.join(self._fc_path, log.fc_path.strip("/"))} to {dest_filepath}")
        remote_file = open(os.path.join(self._fc_path, log.fc_path.strip("/")), "rb")
        if resume:
            # Read corresponding bytes from the log
            offset = dest_file.tell()
            remote_file.seek(offset)
            remote_bytes = remote_file.read(self.RESUME_OVERLAP_CHECK_SIZE)
            if (len(remote_bytes) != self.RESUME_OVERLAP_CHECK_SIZE):
                print("WARN: Remote file is smaller than expected, will re-download")
                resume = False
            else:
                # Find how many bytes overlap, from the start of each buffer
                overlap = 0
                for i in range(self.RESUME_OVERLAP_CHECK_SIZE):
                    if remote_bytes[i] != last_bytes[i]:
                        print(f"Found mismatch at {i}")
                        break
                    else:
                        overlap += 1
                if overlap < self.RESUME_REQUIRED_OVERLAP:
                    print("WARN: Overlap is too small, will re-download")
                    resume = False
                else:
                    # Seek to the first byte that doesn't match
                    remote_file.seek(offset + overlap)
                    dest_file.seek(offset + overlap)
                    print("Resuming from offset", offset + overlap)
        else:
            remote_file.seek(0)
            dest_file.seek(0)
        
        success = False
        dest_file.truncate()
        # Read until EOF
        for retry_count in range(self.DOWNLOAD_RETRIES):
            if retry_count > 0:
                print(f"Retrying, attempt ({retry_count+1}/{self.DOWNLOAD_RETRIES})")
            while True:
                data = remote_file.read(self.DOWNLOAD_BLOCK_SIZE)
                if not data:
                    break
                dest_file.write(data)
            # Validate checksum
            dest_file.seek(0)
            local_checksum = compute_file_crc32(dest_file)
            remote_checksum = self.check_crc32(log)
            if local_checksum == remote_checksum:
                success = True
                break
            else:
                print(f"Checksum mismatch: local {local_checksum} remote {remote_checksum}")
                dest_file.seek(0)
                remote_file.seek(0)
        
        dest_file.close()
        remote_file.close()
        if not success:
            os.remove(dest_filepath)
            return None

        return DownloadedLog(log.fc_path, log.size, local_checksum)

    def delete_log(self, log: LogSummary) -> bool:
        raise NotImplementedError

if __name__ == "__main__":
    log_lib = LogServer("/home/oystub/mnt/fuse/fs/microsd/log")
    print("Listing logs:")
    logs = log_lib.list_logs()
    for i, log in enumerate(logs):
        print(f"{i}: {log.fc_path} ({log.size/1e6:.2f} MB)")

    idx = input("Enter index of log to download:")
    try:
        idx = int(idx)
    except ValueError:
        print("Invalid index")
        exit(1)
    if idx < 0 or idx >= len(logs):
        print("Invalid index")
        exit(1)
    
    output_filepath = input("Enter output filepath:")

    resume = input("Resume download? (y/n):")
    if resume.lower() == "y" or resume.lower() == "":
        resume = True
    else:
        resume = False

    downloaded_log = log_lib.download_log(logs[idx], output_filepath, resume=resume)