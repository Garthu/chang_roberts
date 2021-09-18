import sys
import time
import socket
import fcntl, os
import errno
import random

from time import sleep
from message import Message
from datetime import timedelta
from multiprocessing import Semaphore, Barrier
from concurrent.futures import ProcessPoolExecutor, process


BASE_PORT = 7840

def receive_data(sock_in, sock_out, index):
    ntrials = (process_number * 5)
    while (ntrials):
        if (ntrials != process_number * 4 and ntrials % process_number == 0):
            data = Message("a", sender = index)
            sock_out.send(data.encode())
        msg = b""
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
            break

    data = Message()
    data.decode(msg)

    return data

def election_message(data, sock_out, index):
    if (data.sender == index):
        data.set(code = "w", greatest_process = data.greatest_process)
    elif (data.greatest_process < index):
        data.set(greatest_process = index)
    sock_out.send(data.encode())

def winner_message(data, sock_out, index):
    if (data.sender == index):
        data = pick_message(index)
    sock_out.send(data.encode())
    

def pick_message(index):
    if (index == broken_process):
        while 1:
            sleep(10)
    if (random.randint(1, 10) == 1):
        msg = Message("e", sender = index)
    else:
        msg = Message("a", sender = index)
    
    return msg

def alive_message(data, sock_out, index):
    msg = pick_message(index)
    sock_out.send(msg.encode())
    
def restore_message(data, sock_out, index):
    if ((index + 1) % process_number != data.greatest_process):
        sock_out.send(data.encode())
    else:
        sock_out.close()
        out_address = ('localhost', BASE_PORT + data.sender)
        sock_out = socket.create_connection(out_address)

        msg = pick_message(index)
        sock_out.send(msg)
    
    return sock_out

def node_behavior(sock_in, sock_out, index):
    base_time = timedelta(seconds=process_number)
    danger_time = timedelta(seconds=process_number * 3)
    last_received_time = None
    electing = False

    second_chance = False

    if index == process_number - 1:
        msg = Message("e", index, index)
        sock_out.send(msg.encode())

    while True:
        data = receive_data(sock_in, sock_out, index)
        print(f'Index: {index}\ndata:{data.code}\n')

        reset_time = last_received_time
        last_received_time = time.monotonic()

        if (data.code == "e"):
            election_message(data, sock_out, index)
        elif (data.code == "w"):
            winner_message(data, sock_out, index)
        elif (data.code == "a"):
            alive_message(data, sock_out, index)
        elif (data.code == "r"):
            sock_out = restore_message(data, sock_out, index)
        else:
            print('RESTAURA')
            """
            last_received_time = reset_time
            difference_time = timedelta(seconds=time.monotonic() - last_received_time)
            
            if (difference_time > base_time and difference_time < danger_time):
                msg = Message("a")
                sock_out.send(msg.encode(msg))
            elif (difference_time >= danger_time):
            """


            msg = Message("r", (index - 1) % process_number, index)
            sock_out.send(msg.encode())

            sock_in.close()
            
            in_address = ('localhost', BASE_PORT + index)
            
            sock_in = socket.create_server(in_address, family=socket.AF_INET)
            sock_in.listen(1)
            sock_in, addr = sock_in.accept()

            fcntl.fcntl(sock_in, fcntl.F_SETFL, os.O_NONBLOCK)

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

    node_behavior(sock_in, sock_out, index)

def main(argv):
    global barrier
    global process_number
    global broken_process

    start_time = time.monotonic()
    process_number = int(argv[1])
    barrier = Barrier(process_number)

    try:
        broken_process = int(argv[2])
    except IndexError:
        broken_process = -1
    
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