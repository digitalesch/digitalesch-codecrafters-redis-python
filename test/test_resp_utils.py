# test/test_resp_utils.py
import socket
from app import main
import functools

REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379

# --- TEST UTILS ---

def clear_key_before_test():
    send_raw_redis_command(["RESET"])

def send_raw_redis_command(command: list[str]) -> str:
    with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=3) as sock:
        sent_message = main.encode_resp_command(command)
        sock.sendall(sent_message)
        response = sock.recv(4096)
        print(f"Sent: {sent_message} -> Received: {response}")
        return response
    
def run_multiple_commands(args: tuple[list,bytes]):
    clear_key_before_test()

    for command, expected_response in args:
        response = send_raw_redis_command(command)
        assert response == expected_response

def multi_command_test(test_func):
    """Decorator to run a test that sends multiple Redis commands and checks expected responses."""
    @functools.wraps(test_func)
    def wrapper():
        print(f"Executing: {test_func.__name__}")
        args = test_func()  # The test returns the (command, expected_response) list
        run_multiple_commands(args)
    return wrapper

# --- ACTUAL TESTS, functions that start with test_XXXX ---

@multi_command_test
def test_simple_ping():
    return [
        (["PING"],b"+PONG\r\n")
    ]

@multi_command_test
def test_dual_rpush():
    return [
        (["RPUSH","list_key","banana","pear","pineapple","nstrawberry","nblueberry"], b':5\r\n'),
        (["RPUSH","list_key","test","test2"], b':7\r\n')
    ]

@multi_command_test
def test_ping_rpush__get():
    return [
        (["PING"],b'+PONG\r\n'),
        (["RPUSH","list_key","banana","pear","pineapple","strawberry","blueberry"], b':5\r\n'),
        (["GET","list_key"], b'*5\r\n$6\r\nbanana\r\n$4\r\npear\r\n$9\r\npineapple\r\n$10\r\nstrawberry\r\n$9\r\nblueberry\r\n')
    ]

@multi_command_test
def test_set_type_echo():
    # tests out TYPE command
    return [
        (["SET","fruit","mango"],b"+OK\r\n"),
        (["TYPE","fruit"],b"+string\r\n"),
        (["ECHO","heLLo"],b"$5\r\nheLLo\r\n")
    ]

@multi_command_test
def test_complex_xadd():
    return [
        (["XADD","grape","1-1","orange","apple"],b'$3\r\n1-1\r\n'),
        (["XADD","grape","1-2","banana","pear"],b'$3\r\n1-2\r\n'),
        (["GET","grape"],b"-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
    ]

def test_incrementing_xadd_before_reset():
    print("Executing test_incrementing_xadd_before_reset")
    assert send_raw_redis_command(["XADD","grape","1-*","orange","apple"]) == b'$3\r\n1-3\r\n'

@multi_command_test
def test_generating_id_xadd():
    return [
        (["XADD","grape","1-*","orange","apple"],b'$3\r\n1-1\r\n'),
    ]

@multi_command_test
def test_set_expiration():
    return [
        (["SET","strawberry","apple","PX","1"],b"+OK\r\n"), # error in setting value with expiration
        (["GET","strawberry"],b'$5\r\napple\r\n'),
    ]

def test_get_after_expired():
    response = send_raw_redis_command(["GET","strawberry"])
    print(response)
    assert response == b'$-1\r\n'

# tests out setting a value
@multi_command_test
def test_set_rpush_rpush_lpop_get():
    return [
        (["SET","strawberry","apple","PX","100"],b"+OK\r\n"), # error in setting value with expiration
        (["RPUSH","list_key","banana","pear"],b":2\r\n"),
        (["GET","list_key"],b"*2\r\n$6\r\nbanana\r\n$4\r\npear\r\n"),
        (["RPUSH","list_key","a","b","c"],b":5\r\n"),
        (["LPOP","list_key"],b'$1\r\na\r\n'),
        (["GET","list_key"],b'*4\r\n$1\r\nb\r\n$1\r\nc\r\n$6\r\nbanana\r\n$4\r\npear\r\n'),
    ]
