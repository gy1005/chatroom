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
alive_servers_lock = threading.Lock()
msg_log_lock = threading.Lock()


def heartbeats_send(server_id):
    while True:
        new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        new_sock.settimeout(0.5)
        new_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            new_sock.connect(('localhost', server_id + 20000))
        except:
            alive_servers_lock.acquire()
            if server_id in alive_servers:
                alive_servers.remove(server_id)
            alive_servers_lock.release()
            new_sock.close()
            time.sleep(0.5)
            continue

        try:
            new_sock.sendall("heartbeats_req\n")
        except socket.timeout:
            alive_servers_lock.acquire()
            if server_id in alive_servers:
                alive_servers.remove(server_id)
            alive_servers_lock.release()
            new_sock.close()
            continue
        try:
            buf = new_sock.recv(1024)
            resps = buf.split('\n')

            for resp in resps:
                alive_servers_lock.acquire()
                if resp == "heartbeats_resp":
                    if server_id not in alive_servers:
                        alive_servers.append(server_id)
                # else:
                #     if server_id in alive_servers:
                #         alive_servers.remove(server_id)
                alive_servers_lock.release()
        except socket.timeout:
            alive_servers_lock.acquire()
            if server_id in alive_servers:
                alive_servers.remove(server_id)
            alive_servers_lock.release()
            new_sock.close()

            continue
        new_sock.close()
        time.sleep(0.5)




def master_conn_handler(conn):
    while True:
        buf = conn.recv(1024)
        requests = buf.split('\n')
        # request = conn.recv(1024)
        # print "server " + str(process_id)
        # print requests
        for request in requests:
            if request[0:3] == 'get':
                send_str = ''
                msg_log_lock.acquire()
                for i in range(len(msg_log)):
                    if i == 0:
                        send_str += msg_log[i]
                    else:
                        send_str += ',' + msg_log[i]
                msg_log_lock.release()
                conn.sendall("messages " + send_str + '\n')
                # print "messages " + send_str


            elif request[0:5] == "alive":
                send_str = ''
                alive_servers_lock.acquire()
                sorted_alive_servers = sorted(alive_servers)
                alive_servers_lock.release()
                for i in range(len(sorted_alive_servers)):
                    if i == 0:
                        send_str += str(sorted_alive_servers[i])
                    else:
                        send_str += ',' + str(sorted_alive_servers[i])
                conn.sendall("alive " + send_str + '\n')

            elif request[0:9] == 'broadcast':
                msg_log_lock.acquire()
                msg_log.append(request[10:])
                msg_log_lock.release()
                alive_servers_lock.acquire()
                sorted_alive_servers = alive_servers
                alive_servers_lock.release()

                # print msg_log
                # print sorted_alive_servers
                for i in sorted_alive_servers:
                    if i != process_id:

                        new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        new_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        new_sock.connect(('localhost', i + 20000))
                        new_sock.sendall("message " + request[10:] + '\n')
                        new_sock.close()

                        # print "server" + process_id +" broadcasts message: " + request[10:] + '\n'




def server_conn_handler(server_socket):
    while True:
        conn, addr = server_socket.accept()
        buf = conn.recv(1024)
        requests = buf.split('\n')
        for request in requests:
            if request == "heartbeats_req":
                conn.sendall("heartbeats_resp\n")
            elif request[0:7] == "message":
                msg_log_lock.acquire()
                msg_log.append(request[8:])
                msg_log_lock.release()
                # print msg_log


def main():
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    master_socket.bind((address, port))
    master_socket.listen(5)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((address, 20000 + process_id))
    server_socket.listen(5)

    for i in range(num_server):
        if i != process_id:
            t = threading.Thread(target=heartbeats_send, args=(i,))
            t.start()

    t_recv = threading.Thread(target=server_conn_handler, args=(server_socket,))
    t_recv.start()

    while True:
        conn, addr = master_socket.accept()
        client_handler = threading.Thread(target=master_conn_handler, args=(conn,))
        client_handler.start()



if __name__ == '__main__':
    assert (len(sys.argv) > 3)
    process_id = int(sys.argv[1])
    num_server = int(sys.argv[2])
    port = int(sys.argv[3])
    for i in range(num_server):
        alive_servers.append(i)
    main()
