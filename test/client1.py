import socket
import time

HOST = '127.0.0.1'  # Localhost
PORT = 6379        # Port the server is listening on

def send_message(socket, binary_string: bytes):
    print(f"Sent bytes: '{binary_string}'")
    socket.sendall(binary_string)
    print(f"Received bytes: {s.recv(1024)}")


    
messages = [
    # tests out TYPE command
    # b"*3\r\n$3\r\nSET\r\n$5\r\nfruit\r\n$5\r\nmango\r\n",
    # b"*2\r\n$4\r\nTYPE\r\n$5\r\nfruit\r\n",

    # tests out the XADD command
    b"*7\r\n$4\r\nXADD\r\n$5\r\nmango\r\n$15\r\n1697639913000-0\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbar\r\n$3\r\nfoo\r\n",
    # b"*5\r\n$4\r\nXADD\r\n$5\r\nmango\r\n$3\r\n0-2\r\n$3\r\neit\r\n$3\r\nbar\r\n",
    # b"*2\r\n$4\r\nTYPE\r\n$5\r\nmango\r\n",
    # b"*2\r\n$4\r\nTYPE\r\n$5\r\nmangu\r\n",
    # b"*2\r\n$3\r\nGET\r\n$10\r\nstrawberry\r\n",
    b"*7\r\n$4\r\nXADD\r\n$5\r\nmango\r\n$15\r\n1697639913001-1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbar\r\n$3\r\nfoo\r\n",
    b"*7\r\n$4\r\nXADD\r\n$5\r\nmango\r\n$15\r\n1697639913004-2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbar\r\n$3\r\nfoo\r\n",
    b"*2\r\n$3\r\nGET\r\n$5\r\nmango\r\n",

    # b"*5\r\n$3\r\nSET\r\n$10\r\nstrawberry\r\n$4\r\npear\r\n$2\r\nPX\r\n$3\r\n100\r\n",
    # b"*4\r\n$6\r\nLRANGE\r\n$5\r\nmango\r\n$1\r\n0\r\n$1\r\n3\r\n",
    # b"*4\r\n$6\r\nLRANGE\r\n$5\r\nmango\r\n$1\r\n0\r\n$2\r\n-2\r\n",
    # b"*3\r\n$5\r\nBLPOP\r\n$8\r\nlist_key\r\n$1\r\n5\r\n",
    # b"*5\r\n$3\r\nSET\r\n$10\r\nstrawberry\r\n$5\r\napple\r\n$2\r\nPX\r\n$3\r\n100\r\n",
    # b"*7\r\n$5\r\nRPUSH\r\n$8\r\nlist_key\r\n$6\r\nbanana\r\n$4\r\npear\r\n$9\r\npineapple\r\n$10\r\nstrawberry\r\n$9\r\nblueberry\r\n",
    # b"*5\r\n$5\r\nLPUSH\r\n$8\r\nlist_key\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
    # b"*3\r\n$4\r\nLPOP\r\n$8\r\nlist_key\r\n$1\r\n2\r\n",
    # b"*2\r\n$3\r\nGET\r\n$8\r\nlist_key\r\n",
    # b"*3\r\n$5\r\nRPUSH\r\n$9\r\nraspberry\r\n$6\r\nbanana\r\n",
    # b"*5\r\n$5\r\nRPUSH\r\n$5\r\ngrape\r\n$5\r\ngrape\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n",
    # b"*3\r\n$5\r\nRPUSH\r\n$9\r\nraspberry\r\n$10\r\nstrawberry\r\n",
    # b"*5\r\n$3\r\nSET\r\n$5\r\nfruit\r\n$6\r\norange\r\n$2\r\nPX\r\n$4\r\n3000\r\n",
    # b"*2\r\n$4\r\nECHO\r\n$64\r\nthis is a long string, being tested against 1234 whatever i want\r\n",
    # b"*3\r\n$3\r\nSET\r\n$6\r\norange\r\n$9\r\npineapple\r\n",
    # # b"*2\r\n$3\r\nGET\r\n$6\r\norange\r\n",
    # b"*3\r\n$5\r\nRPUSH\r\n$10\r\nfruit_list\r\n$5\r\napple\r\n",
    # # b"*2\r\n$3\r\nGET\r\n$10\r\nfruit_list\r\n",
    # b"*3\r\n$5\r\nRPUSH\r\n$10\r\nfruit_list\r\n$6\r\nbanana\r\n",
    # b"*3\r\n$5\r\nRPUSH\r\n$10\r\nfruit_list\r\n$4\r\npear\r\n",
    # b"*2\r\n$3\r\nGET\r\n$10\r\nfruit_list\r\n",
]

for message in messages:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        send_message(s,message)
    # time.sleep(2)