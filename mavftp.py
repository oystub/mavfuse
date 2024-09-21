import asyncio
from enum import Enum
import struct
import time
from dataclasses import dataclass

from pymavlink import mavutil

ftp_lock = asyncio.Lock()
ftp_q = asyncio.Queue()
heartbeat_q = asyncio.Queue()

class MavFtpOpcode(Enum):
    NONE = 0
    TERMINATE_SESSION = 1
    LIST_DIRECTORY = 3
    OPEN_FILE_RO = 4
    CREATE_FILE = 6
    WRITE_FILE = 7
    OPEN_FILE_WO = 11
    BURST_READ_FILE = 15
    ACK = 128
    NAK = 129


class MavFtpError(Enum):
    NONE = 0
    FAIL = 1
    FAILERRNO = 2
    INVALIDDATASIZE = 3
    INVALIDSESSION = 4
    NOSESSIONSAVAILABLE = 5
    EOF = 6
    UNKNOWNCOMMAND = 7
    FAIL_FILE_EXISTS = 8
    FAIL_FILE_PROTECTED = 9
    FAIL_FILE_NOT_FOUND = 10


@dataclass
class MavFtpPayload:
    seq_number: int = 0
    session: int = 0
    opcode: MavFtpOpcode = MavFtpOpcode.NONE
    size: int = 0
    req_opcode: MavFtpOpcode = MavFtpOpcode.NONE
    burst_complete: int = 0
    padding: int = 0
    offset: int = 0
    data: bytes = b''

    MAV_FTP_MAX_DATA_LEN = 239
    MAV_FTP_PACKET_LEN = 251  # 12 bytes fixed fields + 239 bytes data

    _STRUCT_FORMAT = '<HBBBBBBI'  # Format: Little-endian, unsigned types

    @classmethod
    def from_bytes(cls, payload: bytes) -> 'MavFtpPayload':
        """
        Creates a MavFtpPayload instance by decoding the given payload.

        :param payload: The bytes object containing the payload to decode.
        :return: A MavFtpPayload instance.
        """
        fixed_size = struct.calcsize(cls._STRUCT_FORMAT)
        fixed_part = payload[:fixed_size]
        (seq_number, session, opcode, size, req_opcode,
         burst_complete, padding, offset) = struct.unpack(cls._STRUCT_FORMAT, fixed_part)
        data = payload[fixed_size:fixed_size + cls.MAV_FTP_MAX_DATA_LEN]
        data = data[:size]
        return cls(
            seq_number=seq_number,
            session=session,
            opcode=MavFtpOpcode(opcode),
            size=size,
            req_opcode=MavFtpOpcode(req_opcode),
            burst_complete=burst_complete,
            padding=padding,
            offset=offset,
            data=data
        )

    def encode(self) -> bytes:
        """
        Encodes the MavFtpPayload fields into a bytes payload.

        :return: A bytes object representing the encoded payload.
        """
        if isinstance(self.opcode, Enum):
            opcode_val = self.opcode.value
        else:
            opcode_val = self.opcode
        
        if isinstance(self.req_opcode, Enum):
            req_opcode_val = self.req_opcode.value
        else:
            req_opcode_val = self.req_opcode

        fixed_part = struct.pack(
            self._STRUCT_FORMAT,
            self.seq_number,
            self.session,
            opcode_val,
            self.size,
            req_opcode_val,
            self.burst_complete,
            self.padding,
            self.offset
        )
        data = self.data.ljust(self.MAV_FTP_MAX_DATA_LEN, b'\0')
        payload = fixed_part + data
        return payload

    def __repr__(self):
        return (f"MavFtpPayload(seq_number={self.seq_number}, "
                f"session={self.session}, "
                f"opcode={self.opcode}, "
                f"size={self.size}, "
                f"req_opcode={self.req_opcode}, "
                f"burst_complete={self.burst_complete}, "
                f"padding={self.padding}, "
                f"offset={self.offset}, data={self.data})")

async def send_heartbeats(master, interval=1):
    while True:
        master.mav.heartbeat_send(
            type=mavutil.mavlink.MAV_TYPE_GCS,
            autopilot=mavutil.mavlink.MAV_AUTOPILOT_GENERIC,
            base_mode=0,
            custom_mode=0,
            system_status=mavutil.mavlink.MAV_STATE_ACTIVE
        )
        await asyncio.sleep(interval)

async def receive_loop(master):
    while True:
        await asyncio.sleep(0.01)
        while msg := master.recv_match(blocking=False):
            # Run callback for all FTP messages
            if msg.get_type() == 'FILE_TRANSFER_PROTOCOL':
                ftp_q.put_nowait(msg)
            elif msg.get_type() == 'HEARTBEAT':
                heartbeat_q.put_nowait(msg)

async def connect_mavlink(connection_string):
    master = mavutil.mavlink_connection(connection_string, baud=57600)
    
    # Start sending heartbeats in a background task
    asyncio.create_task(send_heartbeats(master))
    asyncio.create_task(receive_loop(master))
    
    # Wait for the first heartbeat response
    print("Waiting for heartbeat...")
    await heartbeat_q.get()
    print(f"Connected to {connection_string}")

    return master


def parse_nak(data) -> (MavFtpError):
    if len(data) > 0:
        return MavFtpError(data[0])
    return None

def decode_directory_listing(data):
    """Decode the directory listing from the data."""
    entries = data.split(b'\0')
    parsed_entries = []
    for entry in entries:
        if entry:
            if entry[0:1] == b'D':
                entry_name = entry[1:].decode('utf-8', errors='ignore')
                parsed_entries.append({"type": "directory", "name": entry_name})
            elif entry[0:1] == b'F':
                parts = entry[1:].split(b'\t')
                if len(parts) == 2:
                    entry_name = parts[0].decode('utf-8', errors='ignore')
                    entry_size = int(parts[1])
                    parsed_entries.append({"type": "file", "name": entry_name, "size": entry_size})
            elif entry[0:1] == b'S':
                parsed_entries.append({"type": "skip", "name": ""})
    return parsed_entries

async def list_directory(master, remote_path):
    async with ftp_lock:
        seq_number = 0
        offset = 0
        completed = False

        files = []

        print(f"Mavlink directory {remote_path}")
        while not completed:
            # Append the remote path to the payload
            path_bytes = remote_path.encode('utf-8')

            payload = MavFtpPayload(seq_number=seq_number, opcode=MavFtpOpcode.LIST_DIRECTORY.value, size=len(path_bytes), offset=offset, data=path_bytes)

            # Send the request to list the directory
            print(f"Sending directory list request: {payload}")
            master.mav.file_transfer_protocol_send(
                target_network=0,
                target_system=master.target_system,
                target_component=master.target_component,
                payload=payload.encode()
            )
            response = await ftp_q.get()
            try:
                while True:
                    ftp_q.get_nowait()
            except asyncio.QueueEmpty:
                pass

            if not response:
                print("Failed to receive directory list response.")
                break
            decoded_response = MavFtpPayload.from_bytes(bytes(response.payload))
            print(f"Received directory list response: {decoded_response}")

            if decoded_response.opcode == MavFtpOpcode.NAK:
                error_code = parse_nak(decoded_response.data)

                if error_code == MavFtpError.EOF:
                    # Directory listings are always terminated with an EOF
                    completed = True
                else:
                    print(f"Error: {error_code.name}")
                break

            if decoded_response.seq_number != seq_number + 1 or decoded_response.req_opcode != MavFtpOpcode.LIST_DIRECTORY:
                print("Unexpected response.")
                break

            files.extend(decode_directory_listing(decoded_response.data))
            seq_number += 1
            offset = max(len(files), 1)

        return files

async def main():
    master = await connect_mavlink('/dev/ttyACM2')
    list_directory_task = asyncio.create_task(list_directory(master, '/'))

    dirs = await list_directory_task
    print(dirs)
    


if __name__ == "__main__":
    asyncio.run(main())
