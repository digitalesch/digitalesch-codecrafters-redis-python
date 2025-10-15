import socket
import threading
from datetime import datetime, timedelta

# --- RESP ENCODING / DECODING ---

def get_resp_operation(binary_char: bytes):
    types_map = {
        "+": 1, "-": 2, ":": 3, "$": 4, "*": 5, "_": 6,
        "#": 7, ",": 8, "(": 9, "!": 10, "=": 11, "%": 12,
        "|": 13, "~": 14, ">": 15,
    }
    char = chr(binary_char)
    if char in types_map:
        return types_map[char]
    raise ValueError(f"Not a valid RESP operation: {char}")

def parse_array(binary_array: bytes):
    array_size = int(binary_array[0][1:])
    if (array_size * 2 + 2) != len(binary_array):
        raise ValueError(f"Not enough values in array, should contain {(array_size * 2 + 2)} elements, but has {len(binary_array)} when parsed into {binary_array}!")
    
    array_strings = binary_array[1:(array_size*2+1)]

    parsed_strings = []

    # checks each pair (size, text) for size completeness
    for i in range(0,len(array_strings),2):
        text_size = int(array_strings[i][1:])
        # check texts for individual sizes are correct
        if text_size != len(array_strings[i+1]):
            raise ValueError(f"Size of string '{array_strings[i+1].decode('utf-8')}' is different from informed in RESP format '{array_strings[i].decode('utf-8')}'")
        parsed_strings.append(array_strings[i+1].decode('utf-8'))

    return parsed_strings

def parse_simple_string(binary_array: bytes):
    return [binary_array[0][1:].decode('utf-8')]

def parse_bulk_string(binary_array: bytes):
    array_size = len(binary_array)

    if array_size == 3:
        text_size = int(binary_array[0][1:])
        if text_size == len(binary_array[1]):
            return [binary_array[1].decode('utf-8')]
    else:
        raise ValueError("Invalid Bulk string format")

    # null string
    return b'$-1\r\n'

def parse_integer(binary_string: bytes):
    pass

def parse_resp_strings(binary_string: bytes):
    operation_function_map = {
        1: parse_simple_string,
        3: parse_integer,
        4: parse_bulk_string,
        5: parse_array
    }

    string_texts = binary_string.split(b"\r\n")
    # check empty array
    if len(string_texts) > 0:
        # check if theres an actual array in there and a value for first entry
        if len(string_texts[0]) > 0:
            operation = get_resp_operation(string_texts[0][0])
            return operation_function_map[operation](string_texts)

        # whatever is weird gets thrown an error
        else:
            raise ValueError("Invalid RESP format")

# --- COMMAND HANDLING ---
def encode_array(list_to_encode: list[str]) -> bytes:
    if list_to_encode:
        buffer = f"*{len(list_to_encode)}\r\n"
        for item in list_to_encode:
            buffer += f"${len(item)}\r\n{item}\r\n"
    
    if not list_to_encode:
        buffer = "*-1\r\n"
    
    return buffer.encode('utf-8')

def encode_integer(integer_to_encode: int) -> bytes:
    #:[<+|->]<value>\r\n
    return f":{str(integer_to_encode)}\r\n".encode("utf-8")

def encode_simple_string(text: str) -> bytes:
    return f"+{text}\r\n".encode('utf-8')

def encode_bulk_string(text: str) -> bytes:
    return f"${len(text)}\r\n{text}\r\n".encode('utf-8')

def encode_simple_error(text: str) -> bytes:
    return f"-{text}\r\n".encode('utf-8')

# def set_command(args: list[str], **kwargs) -> bytes:
def set_command(**kwargs) -> bytes:
    # simple set command, for key -> value to get inputed into dict
    print(f"[SET] {kwargs}")
    if "PX" not in kwargs:
        thread_safe_write(shared_dict, thread_lock, **kwargs)
        return encode_simple_string("OK")
    
    if "type" in kwargs:
        thread_safe_write(shared_dict, thread_lock, **kwargs)
        return encode_simple_string("OK")

    raise ValueError(f"Incompatible parameters. Tried {kwargs}, but needed SET <key> <value> <PX>? <seconds>?")

def get_command(args: list[str]) -> dict:
    print(f"[GET]: {args}")
    read_value = thread_safe_read(shared_dict, thread_lock, args[0])
    print(read_value)
    if len(read_value) == 0:
        return {"type": "none", "value": b"$-1\r\n"}
    if type(read_value) == str:
        return {"type": "string", "value": encode_bulk_string(read_value)}
    if type(read_value) == list:
        if type(read_value[0]) == dict:
            return {"type": "stream", "value": encode_array(read_value)}
        if len(read_value) > 1:
            return {"type": "array", "value": encode_array(read_value)}
        if len(read_value) == 1:
            return {"type": "string", "value": encode_bulk_string(read_value[0])}
    

'''
Idea is to have a key -> pair to append to key
1. use get_command to check if key exists
1a. if not create the list
1b. if positive, append to list
2. return list size
'''
def rpush_command(args: list[str]):
    print(f"[RPUSH]: {args[1:]}")
    
    event = read_blocking_pool()

    kwargs = {
        "key": args[1],
        "values": args[2:]
    }
    
    if read_value := thread_safe_read(shared_dict, thread_lock, kwargs.get("key")):
        kwargs["values"] = kwargs.get("values") + read_value
        set_command(**kwargs)
        if event:
            event.get("event").set()
        return encode_integer(len(kwargs.get("values")))
    else:
        set_command(**kwargs)
        if event:
            event.get("event").set()
        return encode_integer(len(kwargs.get("values")))

def lrange_command(key: str, start: int, stop: int):
    empty_array = b'*0\r\n'
    
    if read_value := thread_safe_read(shared_dict, thread_lock, key):
        # gets size of list inputs
        list_size = len(read_value)

        # tries to reverse the index logic
        if abs(start) > list_size:
            start = 0 
        if start < 0:
            start += list_size
        if stop < 0:
            stop += list_size
        
        
        if start >= list_size or start > stop:
            return empty_array 
        if stop >= list_size:
            stop = list_size

        return encode_array(read_value[start:(stop+1)])
    
    return empty_array

def llen_command(key: str):
    if read_value := thread_safe_read(shared_dict, thread_lock, key):
        return encode_integer(str(len(read_value)))
    return encode_integer(0)

def lpop_command(key:str, elements: int = None):
    if read_value := thread_safe_read(shared_dict, thread_lock, key):
        if len(read_value) > 0:
            if elements:
                removed_elements = []
                for _ in range(elements):
                    removed_elements.append(read_value.pop(0))
                thread_safe_write(shared_dict,thread_lock,key,read_value)
                return encode_array(removed_elements)
            else:    
                removed_element = read_value.pop(0)
                thread_safe_write(shared_dict,thread_lock,key,read_value)
                return encode_bulk_string(removed_element)
        
    return encode_bulk_string("")

def blpop_command(key: str, timeout: int, address):
    event = threading.Event()
    
    add_thread_to_blocking_pool(thread_events_blocking_pool,event,address)
    
    wait_for = timeout if timeout > 0 else None

    if event.wait(timeout=wait_for):
        encoded_array = f"*2\r\n${len(key)}\r\n{key}\r\n".encode('utf-8')
        encoded_array += lpop_command(key)
        print(f"[BLPOP]: '{encoded_array}'")
        return encoded_array
    
    return encode_array(None)

def type_command(args: list[str]):
    data = get_command(args)
    print(f"[TYPE]: {data}")
    return encode_simple_string(data.get("type"))


def xadd_command(key: str, entry_id: str, values: list[str], **kwargs):
    temp_dict = {
        entry_id: {values[i]: values[i+1] for i in range(0,len(values),2)}
    }

    if read_value := thread_safe_read(shared_dict, thread_lock, key):
        latest_entry_id = next(iter(read_value[0]))
        source_timestamp, source_sequence_num = latest_entry_id.split('-')
        target_timestamp, target_sequence_num = entry_id.split('-')
        if all([target_timestamp == "0",target_sequence_num == "0"]):
            return encode_simple_error("ERR The ID specified in XADD must be greater than 0-0")
        if source_timestamp > target_timestamp or source_sequence_num >= target_sequence_num:
            return encode_simple_error("ERR The ID specified in XADD is equal or smaller than the target stream top item")

    rpush_command(['RPUSH',key,temp_dict])
    
    return encode_bulk_string(entry_id)

def handle_command(args: list[str], address) -> bytes:
    """
    Receives list of strings like ['PING'], ['ECHO', 'hey'], etc.
    Returns encoded RESP response.
    """
    if not args:
        return encode_simple_string("ERR Empty command")

    command = args[0].upper()
    if command == "PING":
        return encode_simple_string("PONG")
    elif command == "ECHO" and len(args) > 1:
        return encode_bulk_string(args[1])
    if command == "SET" and len(args) > 2:
        kwargs = {
            "key": args[1],
            "values": args[2:],
            "address": address
        }

        if "PX" in args:
            kwargs["expiration_milliseconds"] = args[-1]
            kwargs["values"] = args[2:-2]
        return set_command(**kwargs)
    if command == "GET" and len(args) > 1:
        return get_command(args[1:]).get("value")
    if command == "RPUSH":
        return rpush_command(args)
    if command == "LPUSH":
        # reverses the parameters and applies the RPUSH
        args = args[0:2] + list(reversed(args[2:]))
        return rpush_command(args)
    if command == "LRANGE":
        kwargs = {
            "key": args[1],
            "start": int(args[2]),
            "stop": int(args[3])
        }
        return lrange_command(**kwargs)
    if command == "LLEN":
        return llen_command(args[1])
    if command == "LPOP":
        kwargs = {
            "key": args[1]
        }
        if len(args[1:]) > 1:
            kwargs['elements'] = int(args[2])
        return lpop_command(**kwargs)
    if command == "BLPOP":
        kwargs = {
            "key": args[1],
            "timeout": float(args[2]),
            "address": address
        }
        return blpop_command(**kwargs)
    if command == "TYPE":
        return type_command(args[1:])
    if command == "XADD":
        kwargs = {
            "key": args[1],
            "entry_id": args[2],
            "values": args[3:]
        }
        return xadd_command(**kwargs)

# --- CLIENT HANDLING ---
def client_thread(connection: socket.socket, address):
    try:
        print(f"[CONNECT]: {address}")
        buffer = b""
        while True:
            part = connection.recv(4096)
            if not part:
                break
            buffer += part
            parsed_buffer = parse_resp_strings(buffer)
            print(f"[RECV]: {parsed_buffer}")
            resp = handle_command(parsed_buffer, address)
            print(f"[SEND]: {resp}")
            connection.sendall(resp)
            buffer = b""  # Reset buffer after processing
    finally:
        connection.close()
        print(f"[DISCONNECT]: {address}")

# --- THREAD LOCKS ---
shared_dict = {}
thread_events_blocking_pool = []
thread_lock = threading.Lock()

def add_thread_to_blocking_pool(thread_events_blocking_pool: list, event: threading.Event, address):
    with thread_lock:
        thread_block_data = {
            "address":address, 
            "blocked_at": datetime.now(),
            "event": event
        }
        thread_events_blocking_pool.append(thread_block_data)

def remove_thread_from_blocking_pool(thread_events_blocking_pool: list):
    with thread_lock:
        thread_events_blocking_pool.pop(0)

def read_blocking_pool():
    with thread_lock:
        if len(thread_events_blocking_pool) > 0:
            return thread_events_blocking_pool.pop()

def thread_safe_write(shared_dict, thread_lock, key, values, expiration_milliseconds=None, **kwargs):
    with thread_lock:
        shared_dict[key] = {"value": values, "expires_at": datetime.now() + timedelta(milliseconds=int(expiration_milliseconds)) if expiration_milliseconds else None}

def thread_safe_read(shared_dict, thread_lock, key):
    read_time = datetime.now()
    with thread_lock:
        print(f"[SEARCH]: {key} in {shared_dict}: @{read_time}")
        if dict_value := shared_dict.get(key):
            if expired_at := dict_value.get("expires_at"):
                if expired_at < read_time:
                    shared_dict.pop(key)
                    return ""
            return dict_value.get("value")
        
        return ""

# --- SERVER ---

def start_server(host="127.0.0.1", port=6379):

    server = socket.create_server((host, port), reuse_port=True)
    print(f"Server listening on {host}:{port}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=client_thread, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_server()
