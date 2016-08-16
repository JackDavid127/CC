#!/usr/bin/python

import sys
from socket import *
import struct

connection = None

def connect(port, host='localhost'):
    sockobj = socket(AF_INET, SOCK_STREAM)
    sockobj.connect((host, port))
    global connection 
    connection = sockobj
    print "Connected to %s, port %s." % (host, port)

def socket_read_n(sock, n):
    buf = ''
    while n > 0:
        data = sock.recv(n)
        if data == '':
            raise RuntimeError('unexpected connection close')
        buf += data
        n -= len(data)
    return buf
    
def get_response(sock):
    len_buf = socket_read_n(sock, 4)
    msg_len = struct.unpack('>L', len_buf)[0]
    msg_buf = socket_read_n(sock, msg_len)
    return msg_buf

def send_msg(sock, msg):
    packed_len = struct.pack('>L', len(msg))
    packed_message = packed_len + msg
    sock.send(packed_message)
    return get_response(sock)

def g(key):
    global connection
    response = send_msg(connection, struct.pack('>IBi', 5, 0, key))
    length, msg_type, found, value_size = struct.unpack('>IBBi', response[:10])
    value = struct.unpack('{0}s'.format(value_size), response[10:])

    assert msg_type == 6, "Got message type %s, expected 6" % (msg_type)
    if found:
        print "Found value: %s" % value
    else:
        print "No result"

def p(key, value):
    global connection
    value = value + '\0'
    msg_len = len(value)
    response = send_msg(connection, struct.pack('>IBii{0}s'.format(msg_len), 9 + msg_len, 1, key, msg_len, value))
    length, msg_type = struct.unpack('>IB', response)
    assert length == len(response) - 4, "Got length %s, expected %s" % (length, len(response) - 4)
    assert msg_type == 7, "Got message type %s, expected 7" % (msg_type)

if __name__ == '__main__':
    arg_count = len(sys.argv)
    port,host = None,None

    if arg_count > 1:
        port = int(sys.argv[1])

    if arg_count > 2:
        host = sys.argv[2]

    if port is not None:
        if host is not None:
            connect(port, host)
        else:
            connect(port)
    else:
        print "Connect with 'connect(port, host)'."
