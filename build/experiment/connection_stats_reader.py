from collections import defaultdict
import json
from math import floor
from os import listdir, makedirs
from os.path import join, isfile
import glob

import matplotlib.pyplot as plt
import numpy

__author__ = 'scott.murray'


class ConnectionStatsReader:
    def __init__(self, paths):
        self.__paths = paths
        self.__all_latency = []
        self.__get_latency = []
        self.__get_txn_latency = []
        self.__put_latency = []
        self.__operations_per_second = []

        self.__total_latency = 0
        self.__mean_latency = 0
        self.__95th_percent_latency = 0
        self.__99th_percent_latency = 0
        self.__min_latency = 0
        self.__max_latency = 0

        self.__total_get_latency = 0
        self.__mean_get_latency = 0
        self.__95th_percent_get_latency = 0
        self.__99th_percent_get_latency = 0
        self.__min_get_latency = 0
        self.__max_get_latency = 0

        self.__total_get_txn_latency = 0
        self.__mean_get_txn_latency = 0
        self.__95th_percent_get_txn_latency = 0
        self.__99th_percent_get_txn_latency = 0
        self.__min_get_txn_latency = 0
        self.__max_get_txn_latency = 0

        self.__total_put_latency = 0
        self.__mean_put_latency = 0
        self.__95th_percent_put_latency = 0
        self.__99th_percent_put_latency = 0
        self.__min_put_latency = 0
        self.__max_put_latency = 0

        self.__total_operations = 0
        self.__operations_per_second_per_client = defaultdict(lambda: [])
        self.__total_operations_per_second = 0

    def process(self):
        for input_path in self.__paths:
            input_files_processed = 0
            files = [join(input_path, f) for f in listdir(input_path) if isfile(join(input_path, f))]
            for file_name in files:
                with open(file_name, 'r') as f:
                    try:
                        data = json.loads(f.read())
                    except Exception as e:
                        print "Error in %s" % file_name
                        print e
                        continue

                    client_id, sample = file_name.rsplit('_', 1)
                    client_id = client_id.rsplit('/', 1)[1]

                    input_files_processed += 1
                    self.__total_operations += data["Samples"]

                    latency = data["Values"]
                    assert data["Samples"] == len(latency)
                    self.__all_latency.extend([op[1] for op in latency])

                    for op in latency:
                        if op[0] == "g":
                            self.__get_latency.append(op[1])
                        elif op[0] == "gt":
                            self.__get_txn_latency.append(op[1])
                        else:
                            assert op[0] == "p"
                            self.__put_latency.append(op[1])

                    self.__operations_per_second_per_client[client_id].append(data["OpsPerSecond"])

            print "Processed %s files for input path %s" % (input_files_processed, input_path)

        self.__get_latency.sort()
        self.__get_txn_latency.sort()
        self.__put_latency.sort()
        self.__all_latency.sort()

        # combined latencies
        assert len(self.__all_latency) == self.__total_operations
        if len(self.__all_latency) == 0:
            return False

        self.__total_operations_per_second = self.__total_operations / float(120)

        for latency in self.__all_latency:
            self.__total_latency += latency
        self.__mean_latency = self.__total_latency / float(self.__total_operations)

        self.__95th_percent_latency = self.__all_latency[int(floor(self.__total_operations * 0.95))]
        self.__99th_percent_latency = self.__all_latency[int(floor(self.__total_operations * 0.99))]
        self.__min_latency = self.__all_latency[0]
        self.__max_latency = self.__all_latency[self.__total_operations - 1]

        # get latencies
        self.__total_get_operations = len(self.__get_latency)
        for latency in self.__get_latency:
            self.__total_get_latency += latency
        self.__mean_get_latency = self.__total_get_latency / float(self.__total_get_operations)

        self.__95th_percent_get_latency = self.__get_latency[int(floor(self.__total_get_operations * 0.95))]
        self.__99th_percent_get_latency = self.__get_latency[int(floor(self.__total_get_operations * 0.99))]
        self.__min_get_latency = self.__get_latency[0]
        self.__max_get_latency = self.__get_latency[self.__total_get_operations - 1]


        # get txn latencies
        self.__total_get_txn_operations = len(self.__get_txn_latency)
        if self.__total_get_txn_operations > 0:
            for latency in self.__get_txn_latency:
                self.__total_get_txn_latency += latency

            if self.__total_get_txn_operations == 0:
                self.__mean_get_txn_latency = 0
            else:
                self.__mean_get_txn_latency = self.__total_get_txn_latency / float(self.__total_get_txn_operations)

            self.__95th_percent_get_txn_latency = self.__get_txn_latency[int(floor(self.__total_get_txn_operations * 0.95))]
            self.__99th_percent_get_txn_latency = self.__get_txn_latency[int(floor(self.__total_get_txn_operations * 0.99))]
            self.__min_get_txn_latency = self.__get_txn_latency[0]
            self.__max_get_txn_latency = self.__get_txn_latency[self.__total_get_txn_operations - 1]


        # put latencies
        self.__total_put_operations = len(self.__put_latency)
        if self.__total_put_operations > 0:
            for latency in self.__put_latency:
                self.__total_put_latency += latency

            self.__mean_put_latency = self.__total_put_latency / float(self.__total_put_operations)
            self.__95th_percent_put_latency = self.__put_latency[int(floor(self.__total_put_operations * 0.95))]
            self.__99th_percent_put_latency = self.__put_latency[int(floor(self.__total_put_operations * 0.99))]
            self.__min_put_latency = self.__put_latency[0]
            self.__max_put_latency = self.__put_latency[self.__total_put_operations - 1]

        return True


    def plot_all_latency(self, title=None, file_path=None):
        plt.plot(self.__all_latency[:int(floor(self.__total_operations * 0.99))])
        plt.ylabel('micro-seconds')
        plt.xlabel("operations")

        if title is not None:
            plt.title(title)

        if file_path is None:
            plt.show()
        else:
            plt.savefig(file_path, bbox_inches='tight')
            plt.clf()

    def plot_get_latency(self, title=None, file_path=None):
        plt.plot(self.__get_latency[:int(floor(self.__total_get_operations * 0.99))])
        plt.ylabel('micro-seconds')
        plt.xlabel("operations")

        if title is not None:
            plt.title(title)

        if file_path is None:
            plt.show()
        else:
            plt.savefig(file_path, bbox_inches='tight')
            plt.clf()

    def plot_get_txn_latency(self, title=None, file_path=None):
        plt.plot(self.__get_latency[:int(floor(self.__total_get_txn_operations * 0.99))])
        plt.ylabel('micro-seconds')
        plt.xlabel("operations")

        if title is not None:
            plt.title(title)

        if file_path is None:
            plt.show()
        else:
            plt.savefig(file_path, bbox_inches='tight')
            plt.clf()

    def plot_put_latency(self, title=None, file_path=None):
        plt.plot(self.__put_latency[:int(floor(self.__total_put_operations * 0.99))])
        plt.ylabel('micro-seconds')
        plt.xlabel("operations")

        if title is not None:
            plt.title(title)

        if file_path is None:
            plt.show()
        else:
            plt.savefig(file_path, bbox_inches='tight')
            plt.clf()

    def to_json(self):
        data = {}

        data["total_latency"] = self.__total_latency
        data["mean_latency"] = self.__mean_latency
        data["95th_percent_latency"] = self.__95th_percent_latency
        data["99th_percent_latency"] = self.__99th_percent_latency
        data["min_latency"] = self.__min_latency
        data["max_latency"] = self.__max_latency

        # get
        data["total_get_latency"] = self.__total_get_latency
        data["mean_get_latency"] = self.__mean_get_latency
        data["95th_percent_get_latency"] = self.__95th_percent_get_latency
        data["99th_percent_get_latency"] = self.__99th_percent_get_latency
        data["min_get_latency"] = self.__min_get_latency
        data["max_get_latency"] = self.__max_get_latency

        # get txn
        data["total_get_txn_latency"] = self.__total_get_txn_latency
        data["mean_get_txn_latency"] = self.__mean_get_txn_latency
        data["95th_percent_get_txn_latency"] = self.__95th_percent_get_txn_latency
        data["99th_percent_get_txn_latency"] = self.__99th_percent_get_txn_latency
        data["min_get_txn_latency"] = self.__min_get_txn_latency
        data["max_get_txn_latency"] = self.__max_get_txn_latency

        # put
        data["total_put_latency"] = self.__total_put_latency
        data["mean_put_latency"] = self.__mean_put_latency
        data["95th_percent_put_latency"] = self.__95th_percent_put_latency
        data["99th_percent_put_latency"] = self.__99th_percent_put_latency
        data["min_put_latency"] = self.__min_put_latency
        data["max_put_latency"] = self.__max_put_latency

        data["total_operations"] = self.__total_operations
        data["total_operations_per_second"] = self.__total_operations_per_second

        return json.dumps(data)



    def __str__(self):
        result = ""

        result += "-------------------\n"
        result += "Total Operations Measured: %s\n" % self.__total_operations
        result += "Operations Per Second: %s\n" % self.__total_operations_per_second
        result += "Mean latency: %s\n" % self.__mean_latency
        result += "Min latency: %s\n" % self.__min_latency
        result += "Max latency: %s\n" % self.__max_latency
        result += "95th %% latency: %s\n" % self.__95th_percent_latency
        result += "99th %% latency: %s\n" % self.__99th_percent_latency

        result += "-------------------\n"
        result += "Total Get Operations Measured: %s\n" % self.__total_get_operations
        result += "Mean Get latency: %s\n" % self.__mean_get_latency
        result += "Min Get latency: %s\n" % self.__min_get_latency
        result += "Max Get latency: %s\n" % self.__max_get_latency
        result += "95th %% Get latency: %s\n" % self.__95th_percent_get_latency
        result += "99th %% Get latency: %s\n" % self.__99th_percent_get_latency

        result += "-------------------\n"
        result += "Total Put Operations Measured: %s\n" % self.__total_put_operations
        result += "Mean Put latency: %s\n" % self.__mean_put_latency
        result += "Min Put latency: %s\n" % self.__min_put_latency
        result += "Max Put latency: %s\n" % self.__max_put_latency
        result += "95th %% Put latency: %s\n" % self.__95th_percent_put_latency
        result += "99th %% Put latency: %s\n" % self.__99th_percent_put_latency

        result += "-------------------\n"
        return result

def local():
    paths = [
        #'/Users/kimbleoperations/Desktop/stuff/uni/mscp/project/causal_clients/scripts/ec/client/test/logs/client/connection/',
        #'/Users/kimbleoperations/Desktop/stuff/uni/mscp/project/causal_clients/scripts/orbe/client/test/logs/client/connection/',
        #'/Users/kimbleoperations/Desktop/stuff/uni/mscp/project/causal_clients/scripts/orbe_delay/client/test/logs/client/connection/',
        #'/Users/kimbleoperations/Desktop/stuff/uni/mscp/project/causal_clients/scripts/bolt_on/client/test/logs/client/connection/'

        # ec
        #base_path + '/first/ec/client/exp1405307207/logs/client/connection/',
        #base_path + '/first/ec/client/exp1405307208/logs/client/connection/',
        #base_path + '/first/ec/client/exp1405307209/logs/client/connection/',
        #base_path + '/first/ec/client/exp1405307210/logs/client/connection/',

        # orbe
        #base_path + '/first/orbe/client/exp1405338966/logs/client/connection/',
        #base_path + '/first/orbe/client/exp1405338967/logs/client/connection/',
        #base_path + '/first/orbe/client/exp1405338968/logs/client/connection/',
        #base_path + '/first/orbe/client/exp1405338969/logs/client/connection/',

        # orbe_delay
        #base_path + '/first/orbedelay/client/exp1405339774/logs/client/connection/',
        #base_path + '/first/orbedelay/client/exp1405339775/logs/client/connection/',
        #base_path + '/first/orbedelay/client/exp1405339776/logs/client/connection/',
        #base_path + '/first/orbedelay/client/exp1405339777/logs/client/connection/',

        # bolt_on
        #base_path + '/first/bolton/client/exp1405343998/logs/client/connection/',
        #base_path + '/first/bolton/client/exp1405343998/logs/client/connection/',
        #base_path + '/first/bolton/client/exp1405343998/logs/client/connection/',
        #base_path + '/first/bolton/client/exp1405343998/logs/client/connection/',
    ]

    reader = ConnectionStatsReader(paths)
    reader.process()
    print reader
    #reader.plot_all_latency()
    #reader.plot_get_latency()
    #reader.plot_put_latency()


def generate_comparison_graph(base_result_path, protocols, num_partitions):

    results = {}

    nice_names = {"ec":"EC","bolt_on":"Bolt-on", "orbe":"Orbe", "orbe_delayed":"Orbe+"}
    colours = ["b", "g", "r", "m"]#, "c"]
    symbols = ["p", "^", "s", "D"]#, "*"]
    s = 0

    for protocol in protocols:
        partitions_to_ops = []
        for p in range(1, num_partitions):
            partitions = p + 1

            output_path = base_result_path + "/%s/%s/" % (protocol, partitions)
            with open(output_path + "%s_connection.json" % partitions, 'r') as f:
                data = json.loads(f.read())

            partitions_to_ops.append(data["total_operations_per_second"])
            results[protocol] = partitions_to_ops

    plot_res = []
    for k,v in results.items():
        c = colours[s]
        partition_range = range(2, num_partitions + 1)
        plot_res.append(plt.plot(partition_range, v, c + symbols[s])[0])
        plt.plot(partition_range, v, c+"--")
        s+=1
        plt.xlabel('Number of partitions')
        plt.xticks(partition_range)
        plt.ylabel('Operations per second')
        plt.title("Protocol client throughput scaling comparison")

    nice_names_result = [nice_names[r] for r in results.keys()]
    plt.legend(plot_res, nice_names_result, loc=2)
    plt.savefig(base_result_path + "/comparison_latency_%sp.pdf" % num_partitions, bbox_inches='tight')


def generate_scalability_graph(base_result_path, protocols, num_partitions):
    partition_range = range(2, num_partitions + 1)
    for protocol in protocols:
        partitions_to_ops = []
        for p in range(1, num_partitions):
            partitions = p + 1

            output_path = base_result_path + "/%s/%s/" % (protocol, partitions)
            with open(output_path + "%s_connection.json" % partitions, 'r') as f:
                data = json.loads(f.read())

            partitions_to_ops.append(data["total_operations_per_second"])

        plt.plot(partition_range, partitions_to_ops)
        plt.xlabel('Number of partitions')
        plt.xticks(partition_range)
        plt.ylabel('Operations per second')
        plt.title("%s protocol scaling" % protocol)

        plt.savefig(base_result_path + "/%s/scaling_latency_%sp.pdf" % (protocol, num_partitions), bbox_inches='tight')
        plt.clf()


def real(num_partitions, generate_base_stats, generate_scalability, generate_comparison):
    base_input_path = '/Users/kimbleoperations/Desktop/stuff/uni/phd/logs'
    base_result_path = '/Users/kimbleoperations/Desktop/stuff/uni/phd/logs'
    
    #protocols = ["orbe_delayed", "orbe", "ec", "bolt_on"]
    protocols = ["orbe_delayed", "orbe"]

    if generate_base_stats:
        #for p in range(1, num_partitions):
        for partitions in range(1, num_partitions+1):
            for protocol in protocols:
                paths = glob.glob(base_input_path + '/%s/%s/logs/*/client/connection/' % (protocol, partitions))  # + '/%s/%s/' % (partitions, protocol))
                
                reader = ConnectionStatsReader(paths)
                if not reader.process():
                    continue

                output_path = base_result_path + "/%s/%s/" % (protocol, partitions)
                try:
                    makedirs(output_path)
                except:
                    pass

                with open(output_path + "%s_connection.json" % partitions, 'w') as f:
                    f.write(reader.to_json())

                reader.plot_all_latency("Protocol %s, %s partitions -- All Latency" % (protocol, partitions), output_path + "all_latency.pdf")
                reader.plot_get_latency("Protocol %s, %s partitions -- Get Latency" % (protocol, partitions), output_path + "get_latency.pdf")
                reader.plot_get_txn_latency("Protocol %s, %s partitions -- Get Txn Latency" % (protocol, partitions), output_path + "get_txn_latency.pdf")
                reader.plot_put_latency("Protocol %s, %s partitions -- Put Latency" % (protocol, partitions), output_path + "put_latency.pdf")

    if generate_scalability:
        generate_scalability_graph(base_result_path, protocols, num_partitions)

    if generate_comparison:
        generate_comparison_graph(base_result_path, protocols, num_partitions)


if __name__ == '__main__':
    #real(2, True, False, False)
    real(8, True, True, True)
    





