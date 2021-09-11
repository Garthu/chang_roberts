import sys
import time
import multiprocessing
import socket
import fcntl, os
import errno
from time import sleep

from socket import AF_INET, SOCK_STREAM
from datetime import timedelta
from multiprocessing import Pool
from multiprocessing.connection import Listener, Client
from concurrent.futures import ProcessPoolExecutor

BASE_PORT = 8000

def node(index, process_number):
    in_address = ('localhost', BASE_PORT + index)
    out_address = ('localhost', BASE_PORT + ((index + 1) % process_number))
    
    inbox = socket.socket(AF_INET, SOCK_STREAM)
    conn_out = socket.socket(AF_INET, SOCK_STREAM)

    inbox.bind(in_address)
    inbox.listen(1)
    inbox.settimeout(5)

    if (index % 2 == 0):
        conn_in, _ = inbox.accept()
        conn_out.connect(out_address)
    else:
        conn_out.connect(out_address)
        conn_in, _ = inbox.accept()

    fcntl.fcntl(conn_in, fcntl.F_SETFL, os.O_NONBLOCK)

    if (index != 0):
        data = f'{index}'
        conn_out.send(data.encode())
    
    ntrials = 5
    while (ntrials):
        try:
            msg = conn_in.recv(1024)
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                sleep(1)
                ntrials -= 1
                continue
            else:
                sys.exit(1)
        else:
            print(f'{index} received from {msg.decode()}')
    if (ntrials == 0):
        print("nao recebeu")

    conn_out.close()
    conn_in.close()

    return index

def main(argv):
    start_time = time.monotonic()
    process_number = int(argv[1])

    with ProcessPoolExecutor(max_workers = process_number) as executor:
        futures = []
        for i in range(process_number):
            futures.append(executor.submit(node, i, process_number))

        for i in range(process_number):
            futures[i].result()

        for i in range(process_number):
            executor.shutdown()
    
    end_time = time.monotonic()
    print()
    #print(f'Tempo total: {timedelta(seconds=end_time - start_time)}')

if __name__ == '__main__':
    main(sys.argv)