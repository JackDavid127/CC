#! /usr/bin/env python

#ssh bw1425n02 "source .bash_profile; ./package/bin/simrunner --p 0 --r 0 --config ./package/build/uni_server_config.json" &

import sys 
import os
from time import sleep
#import subprocess

def read_hosts():
	with open("./package/build/known_hadoop_hosts.txt", 'r') as hosts_file:
		return [s.strip() for s in hosts_file.readlines()]


def get_server_hosts(current_host_index, partition_count, replica_count, hosts):
	experiment_server_host_string = "["

	for r in range(0, replica_count):
		experiment_server_host_string += "["
		for p in range(0, partition_count):
			host = hosts[current_host_index]
			current_host_index += 1
			experiment_server_host_string += "\"" + host + "\","

		experiment_server_host_string = experiment_server_host_string[:-1]
		experiment_server_host_string += "],"
	
	experiment_server_host_string = experiment_server_host_string[:-1]
	experiment_server_host_string += "]"

	return current_host_index, experiment_server_host_string


def poke_server_config(server_hosts):
	with open("./package/build/templates/cluster_server_template.txt", 'r') as cluster_server_template_file:
		server_config = cluster_server_template_file.read()
		server_config = server_config.replace("@HOSTS_TEMPLATE@", server_hosts)

	if not os.path.exists("experiment"):
		os.makedirs("experiment")

	with open("./package/build/experiment/cluster_server_config.json", 'w') as cluster_server_config_file:
		cluster_server_config_file.write(server_config)


def execute_command(cmd):
	print cmd
	os.system(cmd)
	#return subprocess.Popen([cmd], stdout = subprocess.PIPE, stderr = subprocess.STDOUT).communicate()[0]


def start_server(host, partition, replica):
	cmd = "ssh {0} \"source .bash_profile; ./package/bin/simrunner --p {1} --r {2} --config ./package/build/experiment/cluster_server_config.json\" &"
	cmd = cmd.format(host, partition, replica)
	execute_command(cmd)


def start_client(client_host, server_host, client_connections):
	cmd = "ssh {0} \"source .bash_profile; ./package/bin/simrunner --client --port 14600 --host {1} --connections {2}\" &"
	cmd = cmd.format(client_host, server_host, client_connections)
	execute_command(cmd)


def start_servers(hosts, replica_count, partition_count):
	print hosts
	host_index = 0
	for r in range(0, replica_count):
		for p in range(0, partition_count):
			host = hosts[host_index]
			start_server(host, p, r)
			host_index += 1


def start_clients(server_hosts, available_client_hosts, client_connections):
	assert len(available_client_hosts) >= len(server_hosts)

	active_clients = []

	for i in range(0, len(server_hosts)):
		server_host = server_hosts[i]
		client_host = available_client_hosts[i]
		active_clients.append(client_host)
		start_client(client_host, server_host, client_connections)

	return active_clients


def countdown(length, message):
	for x in range(length, 0, -1):
		print message.format(x)
		sleep(1)


def kill_runner(active_hosts, require_killed):
	for host in active_hosts:
		cmd = "ssh {0} 'pkill -9 -f simrunner'"
		cmd = cmd.format(host)
		execute_command(cmd)

def move_connection_logs_to_host_location(active_server_hosts, protocol, num_partitions):
	for host in active_server_hosts:
		output_dir = "./logs/{1}/{2}/logs/{0}/client/connection/".format(host, protocol, num_partitions)
		if not os.path.exists(output_dir):
			os.makedirs(output_dir)
		execute_command("mv ./logs/connection/{0}/* {1}".format(host, output_dir))


def move_delay_logs_to_host_location(active_server_hosts, protocol, num_partitions):
	for host in active_server_hosts:
		output_dir = "./logs/{1}/{2}/logs/{0}/server/delay/".format(host, protocol, num_partitions)
		if not os.path.exists(output_dir):
			os.makedirs(output_dir)
		execute_command("mv ./logs/delay/{0}/* {1}".format(host, output_dir))


def main():
	execute_command("rm -rf ~/logs/ logs.tar.gz")

	partition_count = int(sys.argv[1])	
	replica_count = int(sys.argv[2])
	seconds = int(sys.argv[3])
	#protocol = sys.argv[4]
	client_connections = sys.argv[5]
	hosts = read_hosts()

	#for protocol in ["orbe", "orbe_delayed"]:

	for protocol in ["orbe", "orbe_delayed"]:
		for p in range(partition_count, 8 + 1):
			execute_command("./package/build/build_{0}_server.sh -p {1} -r {2}".format(protocol, p, replica_count))

			execute_command("rm -rf ~/logs/ logs.tar.gz logs_{0}_{1}.tar.gz".format(protocol, p))

			kill_runner(hosts, False)

			current_host_index, experiment_server_host_string = get_server_hosts(0, p, replica_count, hosts)
			poke_server_config(experiment_server_host_string)
			active_server_hosts = hosts[:current_host_index]
			start_servers(active_server_hosts, replica_count, p) 

			countdown(5, "sleeping to allow bootstrap... {0}")

			active_client_hosts = start_clients(active_server_hosts, hosts[current_host_index:], client_connections)

			countdown(seconds, "running experiment... {0}")

			active_hosts = active_server_hosts + active_client_hosts 
			kill_runner(active_hosts, True)

			countdown(5, "sleeping to allow termination... {0}")

			move_connection_logs_to_host_location(active_server_hosts, protocol, p)
			move_delay_logs_to_host_location(active_server_hosts, protocol, p)

			execute_command("tar -pczf logs.tar.gz ./logs/")
			execute_command("mv logs.tar.gz logs_{0}_{1}.tar.gz".format(protocol, p))


if __name__ == '__main__':
	main()




