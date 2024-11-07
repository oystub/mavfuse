import asyncio
import struct
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict
import logging

from pymavlink import mavutil

from received_burst_data import ReceivedBurstData


class MavFtpOpcode(Enum):
    NONE = 0
    TERMINATE_SESSION = 1
    LIST_DIRECTORY = 3
    OPEN_FILE_RO = 4
    READ_FILE = 5
    CREATE_FILE = 6
    WRITE_FILE = 7
    REMOVE_FILE = 8
    CREATE_DIRECTORY = 9
    REMOVE_DIRECTORY = 10
    OPEN_FILE_WO = 11
    TRUNCATE_FILE = 12
    RENAME = 13
    CALC_FILE_CRC32 = 14
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
            self.session if self.session is not None else 0,
            opcode_val,
            self.size,
            req_opcode_val,
            self.burst_complete,
            self.padding,
            self.offset,
        )
        data = self.data.ljust(self.MAV_FTP_MAX_DATA_LEN, b'\0')
        payload = fixed_part + data
        return payload


@dataclass
class MavFtpMessage:
    payload: MavFtpPayload
    target_network: int = 0
    target_system: int = 0
    target_component: int = 0

    timeout_s: float = 0.2
    max_retries: int = 6


class MavFtpClient:
    def __init__(self):
        self._logger = logging.getLogger(__name__)

        self._shutdown_event = asyncio.Event()
        self._conn: Optional[mavutil.mavfile] = None

        self._ftp_lock = asyncio.Lock()
        self._ftp_q = asyncio.Queue()
        self._ftp_burst_q = asyncio.Queue()
        self._heartbeat_q = asyncio.Queue()

        self._session: Optional[int] = None
        self._session_path: Optional[str] = None
        self._session_type: Optional[MavFtpOpcode] = None

        self._ftp_out_q = asyncio.Queue()
        self._awaiting_ack: Dict[int, asyncio.Future] = {}

        self.seq_num: int = 1

    async def _send_message_with_ack(self, msg: MavFtpMessage) -> MavFtpPayload:
        expected_req_opcode = msg.payload.opcode
        for attempt in range(msg.max_retries):
            if attempt > 0:
                self._logger.info(f"Retransmitting {msg.payload.opcode.name}, attempt {attempt + 1}/{msg.max_retries}")
            msg.payload.seq_number = self._create_seq_number()
            response_seq_number = msg.payload.seq_number + 1 % 65536
            self._awaiting_ack[response_seq_number] = asyncio.Future()
            self._conn.mav.file_transfer_protocol_send(
                target_network=msg.target_network,
                target_system=msg.target_system,
                target_component=msg.target_component,
                payload=msg.payload.encode()
            )

            try:
                response = await asyncio.wait_for(self._awaiting_ack[response_seq_number], timeout=msg.timeout_s)
                self._awaiting_ack.pop(response_seq_number)
                if response.req_opcode != expected_req_opcode:
                    self._logger.error(f"Received unexpected req_opcode {response.req_opcode}. Expected {expected_req_opcode}")
                    continue
                if response.opcode not in [MavFtpOpcode.ACK, MavFtpOpcode.NAK]:
                    self._logger.error(f"Received unexpected opcode {response.opcode}. Expected ACK or NAK")
                    continue
                return response
            except asyncio.TimeoutError:
                self._logger.error(f"Timeout while waiting for response to {msg.payload.opcode.name}")
                self._awaiting_ack.pop(response_seq_number).cancel()
                continue

        self._awaiting_ack.pop(response_seq_number).cancel()
        raise asyncio.CancelledError(f"Did not receive valid response for {msg.payload.opcode.name}")

    def _create_seq_number(self) -> int:
        self.seq_num = (self.seq_num + 1) % 65536
        return self.seq_num

    async def connect(self, connection_string) -> None:
        self._conn = mavutil.mavlink_connection(connection_string)
        self.heartbeats_task = asyncio.create_task(self._send_heartbeats())
        self.receive_task = asyncio.create_task(self._receive_loop())

        # Wait for the first heartbeat response
        self._logger.info(f"Connecting to {connection_string}, awaiting heartbeat...")
        await self._heartbeat_q.get()
        self._logger.info(f"Connected to {connection_string}")

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
                        if payload.req_opcode == MavFtpOpcode.BURST_READ_FILE:
                            self._ftp_burst_q.put_nowait(payload)
                        else:
                            if self._awaiting_ack.get(payload.seq_number):
                                self._awaiting_ack[payload.seq_number].set_result(payload)
                            else:
                                self._logger.error(f"Received unexpected sequence number {payload.seq_number}")
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
        entries = data.split(b'\0')
        parsed_entries = []
        for entry in entries:
            if entry:
                if entry[0:1] == b'D':
                    entry_name = entry[1:].decode('ascii', errors='ignore')
                    parsed_entries.append(
                        {"type": "directory", "name": entry_name})
                elif entry[0:1] == b'F':
                    parts = entry[1:].split(b'\t')
                    if len(parts) == 2:
                        entry_name = parts[0].decode('ascii', errors='ignore')
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
            path_bytes = remote_path.encode('ascii')
            payload = MavFtpPayload(
                opcode=MavFtpOpcode.LIST_DIRECTORY,
                size=len(path_bytes),
                offset=offset,
                data=path_bytes)

            try:
                response = await self._send_message_with_ack(MavFtpMessage(payload=payload))

            except asyncio.CancelledError:
                self._logger.error("Did not receive response for LIST_DIRECTORY request.")
                return

            if response.opcode == MavFtpOpcode.NAK:
                error_code = self._parse_nak(response.data)

                if error_code == MavFtpError.EOF:
                    # Directory listings are always terminated with an EOF
                    break
                else:
                    self._logger.error(f"Unexpected error while listing directory \"{remote_path}\": {error_code.name}")
                    return

            files.extend(self._decode_directory_listing(response.data))
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
                opcode=MavFtpOpcode.TERMINATE_SESSION
            )

            try:
                response = await self._send_message_with_ack(MavFtpMessage(payload=payload))
            except asyncio.CancelledError:
                self._logger.error("Did not receive response for TERMINATE_SESSION request.")
                return

            if response.opcode == MavFtpOpcode.NAK:
                error_code = self._parse_nak(response.data)
                self._logger.error(f"Unknown error while closing session: {error_code.name}")
                return

            self._session = None
            self._session_path = None
            self._session_type = None

    async def open_file_ro(self, path) -> bool:
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._open_file_ro(path)

    async def _open_file_ro(self, path) -> bool:
        if self._session is not None and self._session_path == path and self._session_type == MavFtpOpcode.OPEN_FILE_RO:
            # Already open
            return True

        # Close any existing sessions, we only support one session at a time
        await self._close_session()

        path_bytes = path.encode('ascii')

        payload = MavFtpPayload(
            opcode=MavFtpOpcode.OPEN_FILE_RO,
            size=len(path_bytes),
            data=path_bytes,
        )

        try:
            response = await self._send_message_with_ack(MavFtpMessage(payload=payload))
        except asyncio.CancelledError:
            return False

        if response.opcode == MavFtpOpcode.NAK:
            error_code = self._parse_nak(response.data)
            self._logger.log(f"Unknown error while opening file \"{path}\" for writing: {error_code.name}")
            return False

        self._session = response.session
        self._session_path = path
        self._session_type = MavFtpOpcode.OPEN_FILE_RO

        return True

    async def _open_file_wo(self, path) -> bool:
        if self._session is not None and self._session_path == path and self._session_type == MavFtpOpcode.OPEN_FILE_WO:
            # Already open
            return True

        # Close any existing sessions, we only support one session at a time
        await self._close_session()

        path_bytes = path.encode('ascii')

        payload = MavFtpPayload(
            opcode=MavFtpOpcode.OPEN_FILE_WO,
            size=len(path_bytes),
            data=path_bytes,
        )

        try:
            response = await self._send_message_with_ack(MavFtpMessage(payload=payload))
        except asyncio.CancelledError:
            return False

        if response.opcode == MavFtpOpcode.NAK:
            error_code = self._parse_nak(response.data)
            self._logger.log(f"Unknown error while opening file \"{path}\" for writing: {error_code.name}")
            return False

        self._session = response.session
        self._session_path = path
        self._session_type = MavFtpOpcode.OPEN_FILE_WO

        return True

    async def read_file(self, path, offset, size) -> Optional[bytearray]:
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._read_file(path, offset, size)

    async def _read_file(self, path, offset, size) -> Optional[bytearray]:

        # TODO: PX4 seems to send up to 180 packets in a burst (~42 kB)
        # We can't really stop it early, so if we only need a few bytes, we should
        # use regular read_file instead of burst_read_file for better performance
        # At least we should do some caching of the last burst read file to avoid
        # a new large burst read file if we only need a few bytes
        #
        # Or maybe just find a way to stop the burst read early

        if self._session is None or self._session_path != path or self._session_type != MavFtpOpcode.OPEN_FILE_RO:
            if not await self._open_file_ro(path):
                return None
        received_data = ReceivedBurstData(offset, size)

        while True:
            next_missing = received_data.get_next_missing()
            if next_missing is None:
                break
            payload = MavFtpPayload(
                session=self._session,
                opcode=MavFtpOpcode.BURST_READ_FILE,
                offset=next_missing[0],
                seq_number=self._create_seq_number(),
                size=MavFtpPayload.MAV_FTP_MAX_DATA_LEN,
            )
            # We send the burst command separately, as it doesn't have ack response
            self._conn.mav.file_transfer_protocol_send(
                target_network=0,
                target_system=self._conn.target_system,
                target_component=self._conn.target_component,
                payload=payload.encode()
            )

            while True:
                try:
                    decoded_response = await asyncio.wait_for(self._ftp_burst_q.get(), timeout=1)
                except asyncio.TimeoutError:
                    self._logger.error("Timeout while waiting for burst read response")
                    break

                if decoded_response.opcode == MavFtpOpcode.ACK:
                    received_data.mark_completed(decoded_response.offset, decoded_response.size, decoded_response.data)
                    if decoded_response.burst_complete:
                        break
                else:
                    error_code = self._parse_nak(decoded_response.data)
                    if error_code == MavFtpError.EOF:
                        received_data.eof(decoded_response.offset)
                        break
                    self._logger.error(f"Unexpected error while reading file \"{path}\": {error_code.name}")
                    return None

        data = received_data.get_data()
        if len(data) > size:  # Can get too much if burst goes over the requested size
            return data[:size]
        await self._close_session()
        return data

    async def create_file(self, path):
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._create_file(path)

    async def _create_file(self, path):
        if self._session is not None:
            # Need to close the current before creating a new file
            await self._close_session()

        path_bytes = path.encode('ascii')
        payload = MavFtpPayload(
            opcode=MavFtpOpcode.CREATE_FILE,
            size=len(path_bytes),
            data=path_bytes
        )

        try:
            response = await self._send_message_with_ack(MavFtpMessage(payload=payload))
        except asyncio.CancelledError:
            return False

        if response.opcode == MavFtpOpcode.ACK:
            # Response packet contains the session number
            self._session = response.session
            self._session_path = path
            self._session_type = MavFtpOpcode.OPEN_FILE_WO
            return True
        else:
            error_code = self._parse_nak(response.data)
            self._logger.error(f"Unexpected error while creating file \"{path}\": {error_code.name}")
            return False

    async def create_directory(self, path):
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._create_directory(path)

    async def _create_directory(self, path):
        path_bytes = path.encode('ascii')
        payload = MavFtpPayload(
            opcode=MavFtpOpcode.CREATE_DIRECTORY,
            size=len(path_bytes),
            data=path_bytes
        )

        try:
            response = await self._send_message_with_ack(MavFtpMessage(payload=payload))
        except asyncio.CancelledError:
            return False

        if response.opcode == MavFtpOpcode.ACK:
            return True
        else:
            error_code = self._parse_nak(response.data)
            self._logger.error(f"Unexpected error while creating directory \"{path}\": {error_code.name}")
            return False

    async def remove_file(self, path):
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._remove_file(path)

    async def _remove_file(self, path):
        path_bytes = path.encode('ascii')
        payload = MavFtpPayload(
            opcode=MavFtpOpcode.REMOVE_FILE,
            size=len(path_bytes),
            data=path_bytes
        )

        try:
            response = await self._send_message_with_ack(MavFtpMessage(payload=payload))
        except asyncio.CancelledError:
            return False

        if response.opcode == MavFtpOpcode.ACK:
            return True
        else:
            error_code = self._parse_nak(response.data)
            self._logger.error(f"Unexpected error while removing file \"{path}\": {error_code.name}")
            return False

    async def remove_directory(self, path):
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._remove_directory(path)

    async def _remove_directory(self, path):
        path_bytes = path.encode('ascii')
        payload = MavFtpPayload(
            opcode=MavFtpOpcode.REMOVE_DIRECTORY,
            size=len(path_bytes),
            data=path_bytes
        )

        try:
            response = await self._send_message_with_ack(MavFtpMessage(payload=payload))
        except asyncio.CancelledError:
            return False

        if response.opcode == MavFtpOpcode.ACK:
            return True
        else:
            error_code = self._parse_nak(response.data)
            self._logger.error(f"Unexpected error while removing directory \"{path}\": {error_code.name}")
            return False

    async def write(self, path, offset, data):
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._write(path, offset, data)

    async def _write(self, path, offset, data, max_retries=3):
        # Check that the file is valid for writing
        if (self._session is None or self._session_path != path or self._session_type != MavFtpOpcode.OPEN_FILE_WO):
            if not await self._open_file_wo(path):
                return False

        payload = MavFtpPayload(
            session=self._session,
            opcode=MavFtpOpcode.WRITE_FILE,
            offset=offset,
            size=len(data),
            data=data
        )

        retries = 0
        while retries < max_retries:
            if retries > 0:
                self._logger.info(f"Retrying write {retries + 1}/{max_retries}")
            try:
                response = await self._send_message_with_ack(MavFtpMessage(payload=payload, max_retries=1))
            except asyncio.CancelledError:
                return False
            if response.opcode == MavFtpOpcode.ACK:
                return True
            else:
                error_code = self._parse_nak(response.data)
                if error_code == MavFtpError.FAIL_FILE_PROTECTED:
                    self._logger.error(f"Error: File \"{path}\" is protected.")
                    # This can happen somtimes, resolve by closing and reopening the file
                    await self._close_session()
                    await asyncio.sleep(0.1)
                else:
                    self._logger.error(f"Unexpected error while writing file \"{path}\": {error_code.name}")
            retries += 1

    async def rename(self, old_path, new_path):
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._rename(old_path, new_path)

    async def _rename(self, old_path, new_path):
        enc_both = old_path.encode('ascii') + b'\0' + new_path.encode('ascii')
        payload = MavFtpPayload(
            opcode=MavFtpOpcode.RENAME,
            size=len(enc_both),
            data=enc_both
        )

        try:
            response = await self._send_message_with_ack(MavFtpMessage(payload=payload))
        except asyncio.CancelledError:
            return False

        if response.opcode == MavFtpOpcode.ACK:
            return True
        else:
            error_code = self._parse_nak(response.data)
            self._logger.log(f"Unexpected error while moving \"{old_path}\" to \"{new_path}\": {error_code.name}")
            return False

    async def truncate_file(self, path, size):
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._truncate_file(path, size)

    async def _truncate_file(self, path, size):
        path_bytes = path.encode('ascii')
        payload = MavFtpPayload(
            opcode=MavFtpOpcode.TRUNCATE_FILE,
            size=len(path_bytes),
            offset=size,
        )

        try:
            response = await self._send_message_with_ack(MavFtpMessage(payload=payload))
        except asyncio.CancelledError:
            return False

        if response.opcode == MavFtpOpcode.ACK:
            return True
        else:
            error_code = self._parse_nak(response.data)
            self._logger.log(f"Unexpected error while truncating file \"{path}\": {error_code.name}")
            return False

    async def crc32(self, path):
        async with self._ftp_lock:
            await self._clear_ftp_queue()
            return await self._crc32(path)

    async def _crc32(self, path):
        # Ask the vehicle to calculate the CRC32 of the file
        path_bytes = path.encode('ascii')
        payload = MavFtpPayload(
            opcode=MavFtpOpcode.CALC_FILE_CRC32,
            size=len(path_bytes),
            data=path_bytes
        )

        try:
            response = await self._send_message_with_ack(MavFtpMessage(payload=payload, timeout_s=30))
        except asyncio.CancelledError:
            return None

        if response.opcode == MavFtpOpcode.ACK:
            return struct.unpack('<I', response.data)[0]
        else:
            error_code = self._parse_nak(response.data)
            self._logger.log(f"Unexpected error while calculating CRC32 for file \"{path}\": {error_code.name}")
            return None
