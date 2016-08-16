#!/usr/bin/python

import sys
from socket import *
import struct

class Connection:
    def __init__(self, port, host='localhost'):
        self.__port = port
        self.__host = host
        self.__connect()

    def __connect(self):
        print "Attempting to connect to %s, port %s." % (self.__host, self.__port)
        try:
            self.__socket = socket(AF_INET, SOCK_STREAM)
            self.__socket.connect((self.__host, self.__port))
            print "Connected to %s, port %s." % (self.__host, self.__port)
        except: 
            print "Failed to connect to %s, port %s." % (self.__host, self.__port)

    def __socket_read_n(self, n):
        buf = ''
        while n > 0:
            data = self.__socket.recv(n)
            if data == '':
                raise RuntimeError('unexpected connection close')
            buf += data
            n -= len(data)
        return buf
        
    def __get_response(self):
        len_buf = self.__socket_read_n(4)
        msg_len = struct.unpack('>L', len_buf)[0]
        msg_buf = self.__socket_read_n(msg_len)
        return msg_buf

    def __send_msg(self, msg, expect_resp=True):
        try:
            packed_len = struct.pack('>L', len(msg))
            packed_message = packed_len + msg
            self.__socket.send(packed_message)
            if expect_resp:
                return True, self.__get_response()
        except: 
            print "Connection Error!"
            self.__connect()
            return False, None

    def g(self, key):
        success, response = self.__send_msg(struct.pack('>IBi', 5, 0, key))
        if success:
            length, msg_type, found, value = struct.unpack('>IBBi', response)
            assert length == len(response) - 4, "Got length %s, expected %s" % (length, len(response) - 4)
            assert msg_type == 6, "Got message type %s, expected 6" % (msg_type)
            if found:
                print "Found value: %s" % value
            else:
                print "No result"

    def p(self, key, value):
        success, response = self.__send_msg(struct.pack('>IBii', 9, 1, key, value))
        if success:
            length, msg_type = struct.unpack('>IB', response)
            assert length == len(response) - 4, "Got length %s, expected %s" % (length, len(response) - 4)
            assert msg_type == 7, "Got message type %s, expected 7" % (msg_type)

    def s(self):
        self.__send_msg(struct.pack('>IB', 1, 13), False)


connection_instance = None

def g(key):
    global connection_instance
    connection_instance.g(key)

def p(key, value):
    global connection_instance
    connection_instance.p(key, value)

def s():
    global connection_instance
    connection_instance.s()

def main():
    global connection_instance 
    connection_instance = None

    arg_count = len(sys.argv)
    port,host = None,None

    if arg_count > 1:
        port = int(sys.argv[1])

    if arg_count > 2:
        host = sys.argv[2]

    if port is not None:
        if host is not None:
            connection_instance = Connection(port, host)
        else:
            connection_instance = Connection(port)
    else:
        print "Connect with 'connect(port, host)'."
        sys.exit(1)

if __name__ == '__main__':
    main()
    