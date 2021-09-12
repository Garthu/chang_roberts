import sys
import time
import socket
import fcntl, os
import errno
from time import sleep

from datetime import timedelta
from multiprocessing import Semaphore, Barrier
from concurrent.futures import ProcessPoolExecutor

BASE_PORT = 6080


def node(index, process_number):
    in_address = ('localhost', BASE_PORT + index)
    out_address = ('localhost', BASE_PORT + ((index + 1) % process_number))

    sock_in = socket.create_server(in_address, family=socket.AF_INET)
    sock_in.listen(1)

    barrier.wait()

    if (index % 2 == 0):
        sock_out = socket.create_connection(out_address)
        sock_in, addr = sock_in.accept()
    else:
        sock_in, addr = sock_in.accept()
        sock_out = socket.create_connection(out_address)

    fcntl.fcntl(sock_in, fcntl.F_SETFL, os.O_NONBLOCK)

    #if (index != 0):
    data = f'{index}'
    sock_out.send(data.encode())
    
    ntrials = 5
    while (ntrials):
        try:
            msg = sock_in.recv(1024)
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
            break
    if (ntrials == 0):
        print("not received")

    return index

def main(argv):
    global barrier
    start_time = time.monotonic()
    process_number = int(argv[1])
    barrier = Barrier(process_number)
    
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