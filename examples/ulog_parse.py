import io
from typing import Optional, Dict
import struct
from dataclasses import dataclass
import re


@dataclass
class ULog:
    timestamp: int


def parse_ulog(f: io.FileIO) -> Optional[ULog]:
    header = f.read(16)
    magic, version, timestamp = struct.unpack("<7sBQ", header)
    if magic != b"ULog\x01\x12\x35":
        print("Not a ULog file")
        return None
    if version != 1:
        print("Unsupported ULog version")
        return None
    
    # Parse message header
    message_header = f.read(3)
    msg_size, msg_type = struct.unpack("<HB", message_header)
    if msg_type != ord("B"):
        print("Expected flag bits message")
        return None

    # Parse the flag bits message
    flag_bits = f.read(msg_size)
    compat_flags, incompat_flags, *appended_offsets = struct.unpack("<8s8s3Q", flag_bits)
    if compat_flags[0] & 0xFE:
        print("Unknown compat flags. This indicates a newer ULog version with breaking changes")
        return None
    if compat_flags[0] & 1:
        #print("Log contains default parameters message")
        pass
    if incompat_flags[0] & 0xFE:
        print("Unknown incompat flags. This indicates a newer ULog version with breaking changes")
        return None
    if incompat_flags[0] & 1:
        #print("Log contains appended data")
        pass

    parse_definitions(f)
    
    return ULog(timestamp)


def parse_definitions(f: io.FileIO):
    vehicle_status_msg = None
    vehicle_status_id = None
    last_a = 0
    while f.tell() < 100000: # Limit to 100k bytes
        message_header = f.read(3)
        if not message_header:
            break
        msg_size, msg_type = struct.unpack("<HB", message_header)

        if msg_type == ord("I"):
            data = f.read(msg_size)
            if not data:
                break
            info = parse_information_message(data)
            if info is None:
                break
            #print(f"O {info.key} {info.data}")
            continue
        elif msg_type == ord("M"):
            data = f.read(msg_size)
            if not data:
                break
            info = parse_multi_information_message(data)
            if info is None:
                break
            #print(f"M {info.key} {info.data}")
            continue
        elif msg_type == ord("F"):
            data = f.read(msg_size)
            if not data:
                break
            fmt = parse_format_message(data)
            if fmt.name == "vehicle_status":
                vehicle_status_msg = fmt
            continue
        elif msg_type == ord("A"):
            data = f.read(msg_size)
            if not data:
                break
            sub = parse_subscription_message(data)
            if sub is None:
                break
            if sub.name == "vehicle_status":
                vehicle_status_id = sub.msg_id
                break
            last_a = f.tell()
        else:
            f.seek(msg_size, io.SEEK_CUR)

    # Seek to almost the end of the file
    f.seek(-300000, io.SEEK_END)
    resync(f)
    
    while True:
        if vehicle_status_msg is None or vehicle_status_id is None:
            print("Vehicle status message not found")
            break
        
        try:
            message_header = f.read(3)
        except Exception as e:
            print("Error reading", e)
            break
        if not message_header:
            break
        msg_size, msg_type = struct.unpack("<HB", message_header)
        if msg_type == ord("D"):
            try:
                data = f.read(msg_size)
            except Exception as e:
                print("Error reading", e)
                break
            if not data:
                break
            msg_id = struct.unpack("<H", data[:2])[0]
            if msg_id == vehicle_status_id:
                # Get the armed_time field
                field = vehicle_status_msg.fields["armed_time"]
                offset = field.offset
                data = data[2+offset:2+offset+data_types_size[field.type]]
                # It is an uint64_t
                armed_time = struct.unpack("<Q", data)[0]
                print(f"Armed time: {armed_time}")
                break
        else:
            f.seek(msg_size, io.SEEK_CUR)


data_types_size = {
    'int8_t': 1,
    'uint8_t': 1,
    'int16_t': 2,
    'uint16_t': 2,
    'int32_t': 4,
    'uint32_t': 4,
    'int64_t': 8,
    'uint64_t': 8,
    'float': 4,
    'double': 8,
    'bool': 1,
    'char': 1,
}

@dataclass
class InformationMessage:
    key: str
    data_type: str
    array_length: int
    name: str
    data: bytes


def parse_information_message(b: bytes) -> InformationMessage:
    key_len = b[0]
    key = b[1:key_len+1].decode("utf-8")
    if key is None:
        return None
    # Use a regex to parse the data type into type, array_length and name
    # E.g int32_t[3] foo -> int32_t, 3, foo

    # Array length is optional, the other two are required
    pattern = r"(\w+)\s*(?:\[(\d+)\])?\s+(\w+)"
    match = re.match(pattern, key)
    if not match:
        raise ValueError(f"Invalid data type: {key}")
    data_type, array_length, name = match.groups()
    array_length = int(array_length) if array_length else 1

    data = b[key_len+1:]
    if not data:
        return None

    return InformationMessage(key, data_type, array_length, name, data)


# Need to add support for multi information messages


def parse_multi_information_message(b: bytes) -> InformationMessage:
    is_continued = b[0]
    return parse_information_message(b[1:])


@dataclass
class FormatField:
    type: str
    name: str
    offset: int


@dataclass
class FormatMessage:
    name: str
    fields: Dict[str, FormatField]


def parse_format_message(b: bytes):
    # Convert to utf8
    format_message = b.decode("utf-8")
    message_name, fields = format_message.strip().split(":", 1)
    out = FormatMessage(message_name, {})
    if message_name == "vehicle_status":
        offset = 0

        for field in fields.split(";"):
            if field == "":
                continue

            pattern = r"(\w+)\s*(?:\[(\d+)\])?\s+(\w+)"
            match = re.match(pattern, field)
            if not match:
                raise ValueError(f"Invalid field: {field}")
            data_type, array_length, name = match.groups()
            array_length = int(array_length) if array_length else 1

            if data_type not in data_types_size:
                raise ValueError(f"Unknown data type: {data_type}")
            
            out.fields[name] = FormatField(data_type, name, offset)
            offset += data_types_size[data_type] * array_length
    return out
  

@dataclass
class SubscriptionMessage:
    multi_id: int
    msg_id: int
    name: str


def parse_subscription_message(b: bytes):
    # use struct to parse uint8_t multi_id and uint16_t msg_id
    multi_id, msg_id = struct.unpack("<BH", b[:3])
    name = b[3:].decode("utf-8")
    return SubscriptionMessage(multi_id, msg_id, name)


"""
'S': Synchronization message
Synchronization message so that a reader can recover from a corrupt message by searching for the next sync message.

c
struct message_sync_s {
  struct message_header_s header; // msg_type = 'S'
  uint8_t sync_magic[8];
};
sync_magic: [0x2F, 0x73, 0x13, 0x20, 0x25, 0x0C, 0xBB, 0x12]"""

def resync(f: io.FileIO):
    # Use algorithm for finding substring in a stream
    sync_magic = b"\x2F\x73\x13\x20\x25\x0C\xBB\x12"

    # Read 10 MAVLink messages at a time

    while True:
        try:
            data = f.read(239*10)
        except Exception as e:
            print("Error reading", e)
            return
        if not data:
            break
        index = data.find(sync_magic)
        if index != -1:
            # Seek to the end of the sync magic
            f.seek(-len(data) + index + 8, io.SEEK_CUR)
            return
