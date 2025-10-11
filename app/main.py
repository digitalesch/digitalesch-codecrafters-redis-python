# import socket  # noqa: F401
# import threading
# import itertools

# def create_connection_and_listen(connection: socket.socket, address):
#     try:
#         print(f"[CONNECT] {address}")
#         data = b""
#         while True:
#             part = connection.recv(1024)
#             if not part:
#                 break
#             data += part
#             if b"\r\n" in data:
#                 break  # crude RESP end check

#         if not data:
#             return

#         parsed_response = resp_parser(data)
#         print(f"[DEBUG] Parsed: {parsed_response}")
#         connection.sendall(parsed_response)
#     except Exception as e:
#         print(f"[ERROR] {e}")
#     finally:
#         connection.close()


# '''
# RESP encodes bulk strings in the following way:
# $<length>\r\n<data>\r\n

# The dollar sign ($) as the first byte.
# One or more decimal digits (0..9) as the string's length, in bytes, as an unsigned, base-10 value.
# The CRLF terminator.
# The data.
# A final CRLF.

# So the string "hello" is encoded as follows:

# $5\r\nhello\r\n

# The empty string's encoding is:

# $0\r\n\r\n

# It is encoded as a bulk string with the length of negative one (-1), like so:
# $-1\r\n
# '''
# def resp_parser(binary_string: bytes):
#     operation_function_map = {
#         5: parse_string
#     }

#     # first byte for operation
#     operation = resp_operation(binary_string[0])
#     if operation:
#         # gets from second char, since first one gives the operation
#         response = operation_function_map[operation](binary_string[1:])
#         # print(response)
#         # send ECHO encoded message
#         if response["parsed_strings"][0].upper() == "ECHO":
#             return encode_strings(response["parsed_strings"][1:])
#         if response["parsed_strings"][0].upper() == "PING":
#             return encode_strings(["PONG"])

#     if not operation:
#         raise ValueError("Invalid operation sent!")

# def encode_strings(texts: list):
#     is_simple_encoding = False
#     if len(texts) == 1:
#         is_simple_encoding = True

#     operation_encoding = b"+" if is_simple_encoding else b"$"

#     print(texts)

#     if is_simple_encoding:
#         strings_encoding = b''.join([operation_encoding + item.encode('utf-8')+b'\r\n' for item in texts])
#     if not is_simple_encoding:
#         strings_encoding = b''.join([operation_encoding + str(len(item)).encode('utf-8')+b'\r\n'+item.encode('utf-8')+b'\r\n' for item in texts])
#     return strings_encoding

# # Parses the * operation, string
# def parse_string(binary_string: bytes):
#     array_sizes, array_index, parsed_strings = [], 0, []

#     is_range = True
#     for binary_char in binary_string:
#         # 2\n\r$4\n\rEcho\n\r$3\n\rhey
#         # skips \n\r
#         if not chr(binary_char).isprintable():
#             is_range = False
#             continue
#         # checks range delimiter
#         if chr(binary_char) == "$":
#             is_range = True
#             array_index += 1
#         # values can be greater than just one digit long
#         if chr(binary_char).isdigit() and is_range:
#             # for when the indexes being do exist already in the array_sizes, to for instance get 10 or bigger into a single list
#             if len(array_sizes) > array_index:
#                 array_sizes[array_index].append(chr(binary_char))
#             # when no reference in the array is present
#             if len(array_sizes) == array_index:
#                 array_sizes.append([chr(binary_char)])
#         # string values
#         if not is_range:
#             parsed_strings.append(chr(binary_char))

#     # reconstructs the array of string into integers, so slices are possible
#     array_sizes = [int(''.join(item)) for item in array_sizes]

#     """
#         accumulates the strings sizes, so its possible to extract from raw data
#         b"*3\r\n$4\r\nEcHo\r\n$3\r\nhey\r\n$2\r\noi\r\n"
#         [3, 4, 3, 2] ['E', 'c', 'H', 'o', 'h', 'e', 'y', 'o', 'i']
#         {'array_size': 3, 'total_texts': ['E', 'c', 'H', 'o', 'h', 'e', 'y', 'o', 'i'], 'parsed_strings': ['EcHo', 'hey', 'oi']}
#     """
#     cummulative_sum = list(itertools.accumulate([0]+array_sizes[1:]))

#     string_definitions = {
#         "array_size": array_sizes[0],
#         "text_sizes": array_sizes[1:],
#         "raw_text": parsed_strings,
#         "parsed_strings": [''.join(parsed_strings[cummulative_sum[i]:cummulative_sum[i+1]]) for i in range(len(cummulative_sum)-1)]
#     }

#     return string_definitions

# '''
# 1   Simple strings      RESP2 	Simple 	+
# 2   Simple Errors       RESP2 	Simple 	-
# 3   Integers 	        RESP2 	Simple 	:
# 4   Bulk strings 	    RESP2 	Aggregate 	$
# 5   Arrays 	            RESP2 	Aggregate 	*
# 6   Nulls 	            RESP3 	Simple 	_
# 7   Booleans 	        RESP3 	Simple 	#
# 8   Doubles 	        RESP3 	Simple 	,
# 9   Big numbers 	    RESP3 	Simple 	(
# 10  Bulk errors 	    RESP3 	Aggregate 	!
# 11  Verbatim strings 	RESP3 	Aggregate 	=
# 12  Maps 	            RESP3 	Aggregate 	%
# 13  Attributes 	        RESP3 	Aggregate 	|
# 14  Sets 	            RESP3 	Aggregate 	~
# 15  Pushes 	            RESP3 	Aggregate 	>
# '''
# def resp_operation(binary_char: bytes):
#     types_map = {
#         "+": 1, "-": 2, ":": 3, "$": 4, "*": 5, "_": 6,
#         "#": 7, ",": 8, "(": 9, "!": 10, "=": 11, "%": 12,
#         "|": 13, "~": 14, ">": 15,
#     }
#     char = chr(binary_char)
#     if char in types_map:
#         return types_map[char]
#     raise ValueError(f"Not a valid RESP type: {char}")

# def main():
#     # You can use print statements as follows for debugging, they'll be visible when running tests.
#     print("Logs from your program will appear here!")

#     # Uncomment this to pass the first stage
#     server_socket = socket.create_server(("localhost", 6379), reuse_port=True)


#     MAX_CONNECTIONS = 10
#     connections_handled = 0

#     while connections_handled < MAX_CONNECTIONS:
#         connection, _ = server_socket.accept()
#         thread = threading.Thread(target=create_connection_and_listen, args=(connection,))
#         thread.start()
#         connections_handled += 1

# if __name__ == "__main__":
#     main()
import socket
import threading

# --- RESP ENCODING / DECODING ---

def parse_resp(data: bytes):
    """
    Parse a RESP array with bulk strings.
    Example input: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    Returns: ['ECHO', 'hey']
    """
    if not data.startswith(b"*"):
        raise ValueError("Expected RESP array")

    lines = data.split(b"\r\n")
    idx = 0

    if lines[idx][0:1] != b"*":
        raise ValueError("Invalid RESP format")
    
    num_elements = int(lines[idx][1:])
    idx += 1

    result = []
    while len(result) < num_elements:
        if lines[idx][0:1] != b"$":
            raise ValueError("Expected bulk string")
        length = int(lines[idx][1:])
        idx += 1
        result.append(lines[idx].decode("utf-8"))
        idx += 1
    return result

def encode_simple_string(text: str) -> bytes:
    return f"+{text}\r\n".encode()

def encode_bulk_string(text: str) -> bytes:
    return f"${len(text)}\r\n{text}\r\n".encode()

def encode_array(items: list[str]) -> bytes:
    resp = f"*{len(items)}\r\n"
    for item in items:
        resp += f"${len(item)}\r\n{item}\r\n"
    return resp.encode()

# --- COMMAND HANDLING ---

def handle_command(args: list[str]) -> bytes:
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
    else:
        return encode_simple_string(f"ERR Unknown command: {command}")

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
            if b"\r\n" in buffer:
                try:
                    args = parse_resp(buffer)
                    print(f"[RECV] {args}")
                    resp = handle_command(args)
                    connection.sendall(resp)
                    buffer = b""  # Reset buffer after processing
                except Exception as e:
                    print(f"[ERROR] {e}")
                    connection.sendall(encode_simple_string("ERR Invalid input"))
                    buffer = b""  # Clear invalid buffer
    finally:
        connection.close()
        print(f"[DISCONNECT] {address}")

# --- SERVER ---

def start_server(host="127.0.0.1", port=6379):
    server = socket.create_server((host, port), reuse_port=True)
    print(f"Server listening on {host}:{port}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=client_thread, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    start_server()
