#! /usr/bin/env python

import os

def main():
	with open("./package/build/known_hadoop_hosts.txt", 'r') as hosts_file:
		hosts = [s.strip() for s in hosts_file.readlines()]

	for host in hosts:
		cmd = "ssh {0} \"pkill -9 -f simrunner\" &"
		cmd = cmd.format(host)
		print cmd
		os.system(cmd)

if __name__ == '__main__':
	main()