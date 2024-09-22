import asyncio
from enum import Enum
import struct
from dataclasses import dataclass

from pymavlink import mavutil
from received_burst_data import ReceivedBurstData
from typing import Optional


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
        try:
            fixed_part = struct.pack(
                self.STRUCT_FORMAT,
                self.seq_number,
                self.session,
                opcode_val,
                self.size,
                req_opcode_val,
                self.burst_complete,
                self.padding,
                self.offset,
            )
        except:
            print(f"Error {self}")
        data = self.data.ljust(self.MAV_FTP_MAX_DATA_LEN, b'\0')
        payload = fixed_part + data
        return payload


class MavFtpClient:
    def __init__(self):
        self._shutdown_event = asyncio.Event()
        self._conn: Optional[mavutil.mavfile] = None

        self._ftp_lock = asyncio.Lock()
        self._ftp_q = asyncio.Queue()
        self._heartbeat_q = asyncio.Queue()

        self._session: Optional[int] = None
        self._session_path: Optional[str] = None
        self._session_type = Optional[MavFtpOpcode] = None

        self.seq_num: int = 1

    def _create_seq_number(self) -> int:
        self.seq_num = (self.seq_num + 1) % 65536
        return self.seq_num

    def _receive_seq_number(self, payload) -> bool:
        diff = (payload.seq_number - self.seq_num) % 65536
        if diff == 0:
            # Duplicate packet
            print(f"Warn: Received duplicate packet with seq number {payload.seq_number}")
            return False
        elif diff == 1:
            # Expected sequence number
            self.seq_num = (payload.seq_number) % 65536
            return True
        elif diff < 10:
            # We assume lost packets, accept the new seq number
            print(f"Warn: Received out-of-order packet. Expected {self.seq_num}, got {payload.seq_number}")
            self.seq_num = (payload.seq_number % 65536)
            return True
        else:
            # Wrong sequence number, possibly old packet. Drop it.
            print(f"Warn: Dropping packet with seq number {payload.seq_number}, expected {self.seq_num}")
            return False

    async def connect(self, connection_string) -> None:
        self._conn = mavutil.mavlink_connection(connection_string, baud=57600)
        self.heartbeats_task = asyncio.create_task(self._send_heartbeats())
        self.receive_task = asyncio.create_task(self._receive_loop())

        # Wait for the first heartbeat response
        print("Waiting for heartbeat...")
        await self._heartbeat_q.get()
        print(f"Connected to {connection_string}")

    async def close(self):
        async with self._ftp_lock:
            await self._close_session()
        self._shutdown_event.set()
        await self.heartbeats_task
        await self.receive_task
        self._conn.close()

    async def _send_heartbeats(self, interval=1) -> None:
        while not self._shutdown_event.is_set():
            self._conn.mav.heartbeat_send(
                type=mavutil.mavlink.MAV_TYPE_GCS,
                autopilot=mavutil.mavlink.MAV_AUTOPILOT_GENERIC,
                base_mode=0,
                custom_mode=0,
                system_status=mavutil.mavlink.MAV_STATE_ACTIVE
            )
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=interval)
            except asyncio.TimeoutError:
                # Continue sending heartbeat after timeout
                continue

    async def _receive_loop(self) -> None:
        while not self._shutdown_event.is_set():
            while msg := self._conn.recv_match(blocking=False):
                # Run callback for all FTP messages
                if msg.get_type() == 'FILE_TRANSFER_PROTOCOL':
                    try:
                        payload = MavFtpPayload.from_bytes(bytes(msg.payload))
                        if self._receive_seq_number(payload):
                            self._ftp_q.put_nowait(payload)
                    except asyncio.QueueFull:
                        pass
                elif msg.get_type() == 'HEARTBEAT':
                    try:
                        self._heartbeat_q.put_nowait(msg)
                    except asyncio.QueueFull:
                        pass
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=0.001)
            except asyncio.TimeoutError:
                continue

    async def _clear_ftp_queue(self) -> None:
        while True:
            try:
                self._ftp_q.get_nowait()
            except asyncio.QueueEmpty:
                return

    @staticmethod
    def _parse_nak(data) -> Optional[MavFtpError]:
        if len(data) > 0:
            return MavFtpError(data[0])
        return None

    @staticmethod
    def _decode_directory_listing(data):
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
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._list_directory(remote_path)

    async def _list_directory(self, remote_path):
        offset = 0
        files = []

        while True:
            path_bytes = remote_path.encode('utf-8')
            payload = MavFtpPayload(seq_number=self._create_seq_number(), opcode=MavFtpOpcode.LIST_DIRECTORY.value, size=len(
                path_bytes), offset=offset, data=path_bytes)
            self._conn.mav.file_transfer_protocol_send(
                target_network=0,
                target_system=self._conn.target_system,
                target_component=self._conn.target_component,
                payload=payload.encode()
            )
            try:
                decoded_response = await asyncio.wait_for(self._ftp_q.get(), timeout=1)
            except asyncio.TimeoutError:
                print("Timed out waiting for directory list response.")
                return

            if decoded_response.req_opcode != MavFtpOpcode.LIST_DIRECTORY:
                print(f"Error: Unexpected response {decoded_response}. Expected req_opcode LIST_DIRECTORY.")
                return

            if decoded_response.opcode != MavFtpOpcode.ACK:
                if decoded_response.opcode == MavFtpOpcode.NAK:
                    error_code = self._parse_nak(decoded_response.data)

                    if error_code == MavFtpError.EOF:
                        # Directory listings are always terminated with an EOF
                        break
                    else:
                        print(f"Error: {error_code.name}")
                        return
                else:
                    print("Error: Unexpected response. Expected opcode ACK or NAK.")
                    return

            files.extend(self._decode_directory_listing(
                decoded_response.data))
            offset = max(len(files), 1)

        return files

    async def close_session(self, session_path) -> None:
        async with self._ftp_lock:
            return await self._close_session(session_path)

    async def _close_session(self, session_path=None) -> None:
        if session_path is not None and self._session_path != session_path:
            # Already closed
            return

        if self._session is not None:
            payload = MavFtpPayload(
                session=self._session,
                seq_number=self._create_seq_number(),
                opcode=MavFtpOpcode.TERMINATE_SESSION
            )
            self._conn.mav.file_transfer_protocol_send(
                target_network=0,
                target_system=self._conn.target_system,
                target_component=self._conn.target_component,
                payload=payload.encode()
            )

            try:
                decoded_response = await asyncio.wait_for(self._ftp_q.get(), timeout=1)
            except asyncio.TimeoutError:
                print("Timed out waiting for TERMINATE_SESSION response.")
                return

            if decoded_response.req_opcode != MavFtpOpcode.TERMINATE_SESSION:
                print("Error: Unexpected response. Expected req_opcode TERMINATE_SESSION.")
                return

            if decoded_response.opcode != MavFtpOpcode.ACK:
                if decoded_response.opcode == MavFtpOpcode.NAK:
                    error_code = self._parse_nak(decoded_response.data)
                    print(f"Error: {error_code.name}")
                    return
                print("Error: Unexpected response. Expected opcode ACK.")
                return
            print(f"Closed session {self._session}")
            self._session = None
            self.session_id = None

    async def open_file_ro(self, path) -> bool:
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._open_file_ro(path)

    async def _open_file_ro(self, path) -> bool:
        if self._session is not None and self._session_path == path:
            # Already open
            return True

        # Close any existing sessions, we only support one session at a time
        await self._close_session()

        path_bytes = path.encode('utf-8')

        payload = MavFtpPayload(
            seq_number=self._create_seq_number(),
            opcode=MavFtpOpcode.OPEN_FILE_RO,
            size=len(path_bytes),
            data=path_bytes,
        )

        self._conn.mav.file_transfer_protocol_send(
            target_network=0,
            target_system=self._conn.target_system,
            target_component=self._conn.target_component,
            payload=payload.encode()
        )

        try:
            response = await asyncio.wait_for(self._ftp_q.get(), timeout=1)
        except asyncio.TimeoutError:
            print("Timed out waiting for response.")
            return False

        if response.req_opcode != MavFtpOpcode.OPEN_FILE_RO:
            print("Error: Unexpected response. Expected req_opcode OPEN_FILE_RO.")
            return False

        if response.opcode != MavFtpOpcode.ACK:
            if response.opcode == MavFtpOpcode.NAK:
                error_code = self._parse_nak(response.data)
                print(f"Error: {error_code.name}")
                return False
            print("Error: Unexpected response. Expected opcode ACK.")
            return False

        self._session = response.session
        self._session_path = path
        self._session_type = MavFtpOpcode.OPEN_FILE_RO

        return True

    async def read_file(self, path, offset, size) -> Optional[bytearray]:
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._read_file(path, offset, size)

    async def _read_file(self, path, offset, size) -> Optional[bytearray]:
        # Need to open the file first, if not already open
        if self._session_path != path or self._session_type != MavFtpOpcode.OPEN_FILE_RO:
            if not await self._open_file_ro(path):
                return None
        received_data = ReceivedBurstData(offset, size)

        while True:
            next_missing = received_data.get_next_missing()
            if next_missing is None:
                break
            payload = MavFtpPayload(
                session=self._session,
                seq_number=self._create_seq_number(),
                opcode=MavFtpOpcode.BURST_READ_FILE,
                offset=next_missing[0],
                size=min(next_missing[1], MavFtpPayload.MAV_FTP_MAX_DATA_LEN),
            )
            self._conn.mav.file_transfer_protocol_send(
                target_network=0,
                target_system=self._conn.target_system,
                target_component=self._conn.target_component,
                payload=payload.encode()
            )

            while True:
                try:
                    decoded_response = await asyncio.wait_for(self._ftp_q.get(), timeout=1)
                except asyncio.TimeoutError:
                    print("Timed out waiting for burst to complete.")
                    return None

                if not decoded_response.req_opcode == MavFtpOpcode.BURST_READ_FILE:
                    print("Error: Unexpected response. Expected req_opcode BURST_READ_FILE.")
                    return None
                if decoded_response.opcode == MavFtpOpcode.ACK:
                    received_data.mark_completed(decoded_response.offset, decoded_response.size, decoded_response.data)
                    if decoded_response.burst_complete:
                        break
                elif decoded_response.opcode == MavFtpOpcode.NAK:
                    error_code = self._parse_nak(decoded_response.data)
                    if error_code == MavFtpError.EOF:
                        received_data.eof(decoded_response.offset)
                        break
                    print(f"Error: {error_code.name}")
                    return None
                else:
                    print("Error: Unexpected response. Expected opcode ACK or NAK.")
                    return None
        data = received_data.get_data()
        return data
