import asyncio
from enum import Enum
import struct
from dataclasses import dataclass

from pymavlink import mavutil


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
    MAV_FTP_PACKET_LEN = 12 + MAV_FTP_MAX_DATA_LEN  # 12 bytes fixed fields
    STRUCT_FORMAT = '<HBBBBBBI'  # Format: Little-endian, unsigned types

    @classmethod
    def from_bytes(cls, payload: bytes) -> 'MavFtpPayload':
        fixed_size = struct.calcsize(cls.STRUCT_FORMAT)
        fixed_part = payload[:fixed_size]
        (seq_number, session, opcode, size, req_opcode,
         burst_complete, padding, offset) = struct.unpack(cls.STRUCT_FORMAT, fixed_part)
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
        if isinstance(self.opcode, Enum):
            opcode_val = self.opcode.value
        else:
            opcode_val = self.opcode

        if isinstance(self.req_opcode, Enum):
            req_opcode_val = self.req_opcode.value
        else:
            req_opcode_val = self.req_opcode

        fixed_part = struct.pack(
            self.STRUCT_FORMAT,
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


class MavFtpClient:
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self.conn = None

        self.ftp_lock = asyncio.Lock()
        self.ftp_q = asyncio.Queue()
        self.heartbeat_q = asyncio.Queue()

    async def connect(self, connection_string):
        self.conn = mavutil.mavlink_connection(connection_string, baud=57600)
        self.heartbeats_task = asyncio.create_task(self.send_heartbeats())
        self.receive_task = asyncio.create_task(self.receive_loop())

        # Wait for the first heartbeat response
        print("Waiting for heartbeat...")
        await self.heartbeat_q.get()
        print(f"Connected to {connection_string}")

    async def close(self):
        self.shutdown_event.set()
        await self.heartbeats_task
        await self.receive_task
        self.conn.close()

    async def send_heartbeats(self, interval=1):
        while not self.shutdown_event.is_set():
            self.conn.mav.heartbeat_send(
                type=mavutil.mavlink.MAV_TYPE_GCS,
                autopilot=mavutil.mavlink.MAV_AUTOPILOT_GENERIC,
                base_mode=0,
                custom_mode=0,
                system_status=mavutil.mavlink.MAV_STATE_ACTIVE
            )
            try:
                await asyncio.wait_for(
                    self.shutdown_event.wait(),
                    timeout=interval)
            except asyncio.TimeoutError:
                # Continue sending heartbeat after timeout
                continue

    async def receive_loop(self):
        while not self.shutdown_event.is_set():
            while msg := self.conn.recv_match(blocking=False):
                # Run callback for all FTP messages
                if msg.get_type() == 'FILE_TRANSFER_PROTOCOL':
                    try:
                        self.ftp_q.put_nowait(msg)
                    except asyncio.QueueFull:
                        pass
                elif msg.get_type() == 'HEARTBEAT':
                    try:
                        self.heartbeat_q.put_nowait(msg)
                    except asyncio.QueueFull:
                        pass
            try:
                await asyncio.wait_for(
                    self.shutdown_event.wait(),
                    timeout=0.001)
            except asyncio.TimeoutError:
                continue

    async def clear_ftp_queue(self):
        while True:
            try:
                self.ftp_q.get_nowait()
            except asyncio.QueueEmpty:
                return

    @staticmethod
    def parse_nak(data) -> (MavFtpError):
        if len(data) > 0:
            return MavFtpError(data[0])
        return None

    @staticmethod
    def decode_directory_listing(data):
        """Decode the directory listing from the data."""
        entries = data.split(b'\0')
        parsed_entries = []
        for entry in entries:
            if entry:
                if entry[0:1] == b'D':
                    entry_name = entry[1:].decode('utf-8', errors='ignore')
                    parsed_entries.append(
                        {"type": "directory", "name": entry_name})
                elif entry[0:1] == b'F':
                    parts = entry[1:].split(b'\t')
                    if len(parts) == 2:
                        entry_name = parts[0].decode('utf-8', errors='ignore')
                        entry_size = int(parts[1])
                        parsed_entries.append(
                            {"type": "file", "name": entry_name, "size": entry_size})
                elif entry[0:1] == b'S':
                    parsed_entries.append({"type": "skip", "name": ""})
        return parsed_entries

    async def list_directory(self, remote_path):
        async with self.ftp_lock:
            # Ensure no old messages are in the queue
            self.clear_ftp_queue()

            seq_number = 0
            offset = 0
            completed = False

            files = []

            print(f"Mavlink directory {remote_path}")
            while not completed:
                path_bytes = remote_path.encode('utf-8')
                payload = MavFtpPayload(seq_number=seq_number, opcode=MavFtpOpcode.LIST_DIRECTORY.value, size=len(
                    path_bytes), offset=offset, data=path_bytes)

                self.conn.mav.file_transfer_protocol_send(
                    target_network=0,
                    target_system=self.conn.target_system,
                    target_component=self.conn.target_component,
                    payload=payload.encode()
                )
                try:
                    response = await asyncio.wait_for(self.ftp_q.get(), timeout=1)
                except asyncio.TimeoutError:
                    print("Timed out waiting for directory list response.")
                    break

                decoded_response = MavFtpPayload.from_bytes(
                    bytes(response.payload))

                if decoded_response.opcode == MavFtpOpcode.NAK:
                    error_code = self.parse_nak(decoded_response.data)

                    if error_code == MavFtpError.EOF:
                        # Directory listings are always terminated with an EOF
                        completed = True
                    else:
                        print(f"Error: {error_code.name}")
                    break

                if decoded_response.seq_number != seq_number + 1 or decoded_response.req_opcode != MavFtpOpcode.LIST_DIRECTORY:
                    print("Error: Unexpected response.")
                    break

                files.extend(self.decode_directory_listing(
                    decoded_response.data))
                seq_number += 1
                offset = max(len(files), 1)

            return files
