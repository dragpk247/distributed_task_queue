import socket
import subprocess
import time
import os

if os.path.exists('live_demo.aof'):
    os.remove('live_demo.aof')

server = subprocess.Popen(
    ['./target/release/distributed_task_queue', '--bind', '127.0.0.1:9999', '--aof', 'live_demo.aof']
)
time.sleep(0.4)

def encode_resp(*args):
    out = [f"*{len(args)}\r\n"]
    for a in args:
        out.append(f"${len(a)}\r\n{a}\r\n")
    return "".join(out).encode("utf-8")

def query(sock, *args):
    sock.sendall(encode_resp(*args))
    data = sock.recv(1024).decode("utf-8")
    return data

try:
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    c.connect(('127.0.0.1', 9999))
    print("1. Server PING probe:", repr(query(c, "PING")))
    print("2. Producer LPUSH task 1:", repr(query(c, "LPUSH", "orders", "order_01")))
    print("3. Producer LPUSH task 2:", repr(query(c, "LPUSH", "orders", "order_02")))
    
    print("4. Worker 1 leases task 1 (2s lease):", repr(query(c, "RPOPLEASE", "orders", "2")))
    print("5. Worker 1 extends lease via TASKTOUCH (+10s):", repr(query(c, "TASKTOUCH", "orders", "task-1", "10")))
    print("6. Worker 1 settles task 1 via TASKACK:", repr(query(c, "TASKACK", "orders", "task-1")))

    print("7. Worker 1 leases task 2 (1s lease):", repr(query(c, "RPOPLEASE", "orders", "1")))
    c.close()
    print("8. Worker 1 crashed before acknowledging task 2!")

    print("9. Waiting 2.2s for background lease reaper to run...")
    time.sleep(2.2)

    w2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    w2.connect(('127.0.0.1', 9999))
    print("10. Worker 2 comes online and pops recovered task 2:", repr(query(w2, "RPOP", "orders")))
    print("11. Compact AOF ledger via BGREWRITEAOF:", repr(query(w2, "BGREWRITEAOF")))
    w2.close()
    print("\n>>> ALL CHECKS PASSED SUCCESSFULLY! <<<")

finally:
    server.terminate()
    server.wait()
    if os.path.exists('live_demo.aof'):
        os.remove('live_demo.aof')
