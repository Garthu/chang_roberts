import sys
import time
import socket
import fcntl, os
import errno
import random

from time import sleep
from message import Message
from multiprocessing import Barrier
from concurrent.futures import ProcessPoolExecutor


BASE_PORT = 7840

# print current process state
def process_log(index, str):
    print(f"[process {index}] {str}")

# try to receive data from socket
def receive_data(sock_in, sock_out, index):
    # number of times to try to receive data
    ntrials = (process_number * 4)

    while (ntrials):
        # didn't receive anything until now. Signal other processes to wait
        # more. Some process may have crashed
        if (ntrials == process_number * 2):
            data = Message("a", greatest_process = process_number, sender = index)
            sock_out.send(data.encode())

        msg = b""
        
        # try to receive data
        try:
            msg = sock_in.recv(5)
        # didn't receive data. Try again later
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                sleep(1)
                ntrials -= 1
                continue
            else:
                sys.exit(1)
        # received data. continue
        else:
            break

    # decode data
    data = Message()
    data.decode(msg)

    return data

# election message handler
def election_message(data, sock_out, index):
    log = f"election running! current winner: {data.greatest_process}"
    # message have gone throught all ring. A new leader was chosen
    if (data.sender == index):
        data.set(code = "w", greatest_process = data.greatest_process)
        log = f"election finished! winner: {data.greatest_process}"
    
    # my index is greater than current election leader
    elif (data.greatest_process < index):
        data.set(greatest_process = index)
        log = f"election running! current winner: {index}"

    process_log(index, log)
    sock_out.send(data.encode())

# winner message handler
def winner_message(data, sock_out, index):
    process_log(index, f"process {data.greatest_process} is the new leader!")
    # message have gone throught all ring
    if (data.sender == index):
        data = pick_message(index)
    sock_out.send(data.encode())

# build random message.
# 10% of chance to send election message.
# 90% of chance to send alive message
def pick_message(index):
    if (random.randint(1, 10) == 1):
        msg = Message("e", index, sender = index)
        process_log(index, f"initiating election! current winner: {index}")
    else:
        msg = Message("a", process_number, sender = index)
    
    return msg

# alive message handler. The process behind me is alive
def alive_message(data, sock_out, index):
    process_log(index, f"process {data.sender} is alive!")
    msg = pick_message(index)
    sock_out.send(msg.encode())

# restore message handler. Some process has crashed
def restore_message(data, sock_out, index):
    # repass message to the tail node of the ring
    if ((index + 1) % process_number != data.greatest_process):
        log = f"process {data.greatest_process} is broken! repassing"
        sock_out.send(data.encode())
    else:
        # I'm the tail node of the ring. Connect to the head to recover the ring
        log = f"process {data.greatest_process} is broken! recovering ring"
        sock_out.close()
        out_address = ('localhost', BASE_PORT + data.sender)
        sock_out = socket.create_connection(out_address)

        # initiate a new election
        msg = Message("e", index, sender = index)
        sock_out.send(msg.encode())
    
    process_log(index, log)
    return sock_out

# simulate an error
def error():
    while 1:
        sleep(10)

def node_behavior(sock_in, sock_out, index):
    # starts the ring with an election
    if index == 0:
        process_log(index, f"initiating election! current winner: {index}")
        msg = Message("e", index, index)
        sock_out.send(msg.encode())

    # node main loop
    while True:
        # try to receive data
        data = receive_data(sock_in, sock_out, index)

        # 20% chance of crashing here if I am the bad process
        if (index == broken_process and random.randint(1, 5) == 1):
            error()

        # handle received data
        if (data.code == "e"):
            # handle election message
            election_message(data, sock_out, index)
        elif (data.code == "w"):
            # handle winner message
            winner_message(data, sock_out, index)
        elif (data.code == "a"):
            # handle alive message
            alive_message(data, sock_out, index)
        elif (data.code == "r"):
            # handle restore message
            sock_out = restore_message(data, sock_out, index)
        else:
            # nothing received. The process behind me has crashed
            msg = Message("r", (index - 1) % process_number, index)

            sock_in.close()
            
            in_address = ('localhost', BASE_PORT + index)
            
            sock_in = socket.create_server(in_address, family=socket.AF_INET)
            sock_in.listen(1)
            sock_out.send(msg.encode())
            sock_in, addr = sock_in.accept()

            fcntl.fcntl(sock_in, fcntl.F_SETFL, os.O_NONBLOCK)

def node(index, process_number):
    # create sockets for receiving and send data
    in_address = ('localhost', BASE_PORT + index)
    out_address = ('localhost', BASE_PORT + ((index + 1) % process_number))

    sock_in = socket.create_server(in_address, family=socket.AF_INET)
    sock_in.listen(1)

    # we need all the nodes to start their server socket to try to connect
    barrier.wait()

    # we need be sure that all process don't begin with same operation
    if (index % 2 == 0):
        sock_out = socket.create_connection(out_address)
        sock_in, addr = sock_in.accept()
    else:
        sock_in, addr = sock_in.accept()
        sock_out = socket.create_connection(out_address)

    # make inbox a non blocking socket. We need that to ensure that the ring
    # continue to run if a process crashes
    fcntl.fcntl(sock_in, fcntl.F_SETFL, os.O_NONBLOCK)

    # run node behavior
    node_behavior(sock_in, sock_out, index)

def main(argv):
    global barrier
    global process_number
    global broken_process

    process_number = int(argv[1])
    barrier = Barrier(process_number)
    try:
        broken_process = int(argv[2])
    except IndexError:
        broken_process = -1
    
    # lauch processes
    with ProcessPoolExecutor(max_workers = process_number) as executor:
        futures = []
        for i in range(process_number):
            futures.append(executor.submit(node, i, process_number))

        for i in range(process_number):
            futures[i].result()

        for i in range(process_number):
            executor.shutdown()

if __name__ == '__main__':
    main(sys.argv)