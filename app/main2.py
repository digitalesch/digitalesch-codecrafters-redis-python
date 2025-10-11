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
        return encode_bulk_string("PONG")
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
