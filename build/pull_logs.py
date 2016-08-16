#! /usr/bin/env python

import os

def syscall(command):
	os.system(command)

def main():
	for protocol in ["orbe", "orbe_delayed"]:
		for partition in range(1, 9):
			archive_name = "logs_{0}_{1}.tar.gz".format(protocol, partition)
			syscall("rm -rf ./logs/connection/")
			syscall("rm -rf ./logs/delay/")
			syscall("rm ./{0}".format(archive_name))
			syscall("scp username@staff.ssh.inf.ed.ac.uk:~/{0} ./{0} > /dev/null".format(archive_name))
			syscall("tar -zxf ./{0} > /dev/null".format(archive_name))
			syscall("rm ./{0}".format(archive_name))

if __name__ == '__main__':
	main()