#!/usr/bin/env python

import socket
import sys
import signal
import os
import threading

global process_id
global num_server
global port

address = 'localhost'
alive_servers = []
msg_log = []


def sigkill_handler(signum, frame):
    exit(0)


def conn_handler(conn):
    request = conn.recv(1024)
    if request[0:2] == 'get':
        send_str = ''
        for i in range(len(msg_log)):
            if i == 0:
                send_str += msg_log[i]
            else:
                send_str += ',' + msg_log[i]
        conn.sendall("messages " + send_str)

    elif request[0:4] == "alive":
        send_str = ''
        sorted_alive_servers = sorted(alive_servers)
        for i in range(len(sorted_alive_servers)):
            if i == 0:
                send_str += str(sorted_alive_servers[i])
            else:
                send_str += ',' + str(sorted_alive_servers[i])
        conn.sendall("alive " + send_str)

    elif request[0:8] == 'broadcast':
        msg_log.append(request[10:])
        for i in alive_servers:
            if i != process_id:
                new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_sock.connect(('localhost', i + 20000))
                new_sock.sendall(request[10:])
                new_sock.close()
    else:
        msg_log.append(request)


def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((address, port))
    s.listen(5)

    for i in range(num_server):
        alive_servers.append(i)

    while True:
        conn, addr = s.accept()
        client_handler = threading.Thread(target=conn_handler, args=(conn,))
        client_handler.start()


if __name__ == '__main__':
    assert (len(sys.argv) > 3)
    process_id = int(sys.argv[1])
    num_server = int(sys.argv[2])
    port = int(sys.argv[3])
    signal.signal(signal.SIGTERM, sigkill_handler)
    main()
