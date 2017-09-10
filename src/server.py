#!/usr/bin/env python

import socket
import sys
import signal
import os
import threading
import time

global process_id
global num_server
global port

address = 'localhost'
alive_servers = []
msg_log = []


def detect_alive_servers(server_id):
    while True:
        new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        new_sock.settimeout(1.0)
        try:
            new_sock.connect(('localhost', server_id + 20000))
        except:
            if server_id in alive_servers:
                alive_servers.remove(server_id)
            new_sock.close()
            time.sleep(1.0)
            continue

        try:
            new_sock.sendall("heartbeats_req")
        except socket.timeout:
            if server_id in alive_servers:
                alive_servers.remove(server_id)
            new_sock.close()
            continue
        try:
            resp = new_sock.recv(1024)
            if resp == "heartbeats_resp":
                if server_id not in alive_servers:
                    alive_servers.append(server_id)
            else:
                if server_id in alive_servers:
                    alive_servers.remove(server_id)
        except socket.timeout:
            if server_id in alive_servers:
                alive_servers.remove(server_id)
            new_sock.close()
            continue
        new_sock.close()
        time.sleep(1.0)




def conn_handler(conn):
    request = conn.recv(1024)
    if request[0:3] == 'get':
        send_str = ''
        for i in range(len(msg_log)):
            if i == 0:
                send_str += msg_log[i]
            else:
                send_str += ',' + msg_log[i]
        conn.sendall("messages " + send_str)

    elif request[0:5] == "alive":
        send_str = ''
        sorted_alive_servers = sorted(alive_servers)
        for i in range(len(sorted_alive_servers)):
            if i == 0:
                send_str += str(sorted_alive_servers[i])
            else:
                send_str += ',' + str(sorted_alive_servers[i])
        conn.sendall("alive " + send_str)
    elif request[0:9] == 'broadcast':
        msg_log.append(request[10:])
        for i in alive_servers:
            if i != process_id:
                new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_sock.connect(('localhost', i + 20000))
                new_sock.sendall(request[10:])
                new_sock.close()
    elif request == 'heartbeats_req':
        conn.sendall("heartbeats_resp")
    else:
        msg_log.append(request)


def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((address, port))
    s.listen(5)

    for i in range(num_server):
        if i != process_id:
            t = threading.Thread(target=detect_alive_servers, args=(i,))
            t.start()


    while True:
        conn, addr = s.accept()
        client_handler = threading.Thread(target=conn_handler, args=(conn,))
        client_handler.start()


if __name__ == '__main__':
    assert (len(sys.argv) > 3)
    process_id = int(sys.argv[1])
    num_server = int(sys.argv[2])
    port = int(sys.argv[3])
    alive_servers.append(process_id)
    main()
