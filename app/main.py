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
        raise ValueError(f"Not enought values in array, should contain {(array_size * 2 + 2)} elements, but has {len(binary_array)} when parsed into {binary_array}!")
    
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
    buffer = f"*{len(list_to_encode)}\r\n"
    for item in list_to_encode:
        buffer += f"${len(item)}\r\n{item}\r\n"
    
    return buffer.encode('utf-8')

def encode_integer(integer_to_encode: int) -> bytes:
    #:[<+|->]<value>\r\n
    return f":{str(integer_to_encode)}\r\n".encode("utf-8")

def encode_simple_string(text: str) -> bytes:
    return f"+{text}\r\n".encode('utf-8')

def encode_bulk_string(text: str) -> bytes:
    return f"${len(text)}\r\n{text}\r\n".encode('utf-8')

# def set_command(args: list[str], **kwargs) -> bytes:
def set_command(**kwargs) -> bytes:
    # simple set command, for key -> value to get inputed into dict
    print(f"Set command: {kwargs}")
    if "PX" not in kwargs:
        thread_safe_write(shared_dict, dict_lock, **kwargs)
        print(shared_dict)
        return encode_simple_string("OK")
    
    if "type" in kwargs:
        thread_safe_write(shared_dict, dict_lock, **kwargs)
        return encode_simple_string("OK")

    raise ValueError(f"Incompatible parameters. Tried {kwargs}, but needed SET <key> <value> <PX>? <seconds>?")

def get_command(args: list[str]) -> bytes:
    read_value = thread_safe_read(shared_dict, dict_lock, args[0])
    print(read_value)
    if len(read_value) == 0:
        return b"$-1\r\n"
    if type(read_value) == str:
        return encode_bulk_string(read_value)
    if type(read_value) == list:
        if len(read_value) > 1:
            return encode_array(read_value)
        if len(read_value) == 1:
            return encode_bulk_string(read_value[0])

'''
Idea is to have a key -> pair to append to key
1. use get_command to check if key exists
1a. if not create the list
1b. if positive, append to list
2. return list size
'''
def rpush_command(args: list[str]):
    print(f"args are: {args}, need to pass {args[1:]}")
    kwargs = {
        "key": args[1],
        "values": args[2:]
    }
    
    if read_value := thread_safe_read(shared_dict, dict_lock, kwargs.get("key")):
        print(f"appending {kwargs}")
        kwargs["values"] = kwargs.get("values") + read_value
        print(kwargs)
        set_command(**kwargs)
        return encode_integer(len(kwargs.get("values")))
    else:
        print(f"creating key {kwargs}")
        set_command(**kwargs)
        return encode_integer(len(kwargs.get("values")))

def lrange_command(key: str, start: int, stop: int):
    empty_array = b'*0\r\n'
    
    if read_value := thread_safe_read(shared_dict, dict_lock, key):
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

        print(list_size,read_value, start, stop, key)
        return encode_array(read_value[start:(stop+1)])
    
    return empty_array

def handle_command(args: list[str]) -> bytes:
    """
    Receives list of strings like ['PING'], ['ECHO', 'hey'], etc.
    Returns encoded RESP response.
    """
    if not args:
        return encode_simple_string("ERR Empty command")

    command = args[0].upper()
    print(command, args)
    if command == "PING":
        return encode_simple_string("PONG")
    elif command == "ECHO" and len(args) > 1:
        return encode_bulk_string(args[1])
    if command == "SET" and len(args) > 2:
        kwargs = {
            "key": args[1],
            "values": args[2:]
        }

        if "PX" in args:
            print('x')
            kwargs["expiration_milliseconds"] = args[-1]
            kwargs["values"] = args[2:-2]
        return set_command(**kwargs)
    if command == "GET" and len(args) > 1:
        return get_command(args[1:])
    if command == "RPUSH":
        return rpush_command(args)
    if command == "LRANGE":
        kwargs = {
            "key": args[1],
            "start": int(args[2]),
            "stop": int(args[3])
        }
        return lrange_command(**kwargs)

# --- CLIENT HANDLING ---

def client_thread(connection: socket.socket, address):
    try:
        print(f"[CONNECT] {address}")
        buffer = b""
        while True:
            part = connection.recv(4096)
            if not part:
                break
            buffer += part
            parsed_buffer = parse_resp_strings(buffer)
            print(f"[RECV] {parsed_buffer}")
            resp = handle_command(parsed_buffer)
            connection.sendall(resp)
            buffer = b""  # Reset buffer after processing
    finally:
        connection.close()
        print(f"[DISCONNECT] {address}")

# --- THREAD LOCK ---
shared_dict = {}
dict_lock = threading.Lock()

def thread_safe_write(shared_dict, dict_lock, key, values, expiration_milliseconds=None):
    with dict_lock:
        shared_dict[key] = {"value": values, "expires_at": datetime.now() + timedelta(milliseconds=int(expiration_milliseconds)) if expiration_milliseconds else None}

def thread_safe_read(shared_dict, dict_lock, key):
    read_time = datetime.now()
    with dict_lock:
        print(f"Searching key {key} in dict: {shared_dict} at {read_time}")
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
