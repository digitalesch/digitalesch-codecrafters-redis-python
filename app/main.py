import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # accept connections
    connection, _ = server_socket.accept()
    while connection.recv(len(b"*1\r\n$4\r\nPING\r\n")):
        # connection.recv(len(b"*1\r\n$4\r\nPING\r\n"))
        connection.sendall(b"+PONG\r\n")

    connection.close()

if __name__ == "__main__":
    main()
