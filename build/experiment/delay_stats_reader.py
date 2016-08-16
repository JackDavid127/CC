import glob
import json
from math import floor
from os import listdir, makedirs
from os.path import join, isfile
import matplotlib.pyplot as plt

__author__ = 'scott.murray'


class DelayStatsReader:
    def __init__(self, paths):
        self.__paths = paths

        self.__latencies = []
        self.__total_latency = 0
        self.__mean_latency = 0
        self.__95th_percent_latency = 0
        self.__99th_percent_latency = 0
        self.__min_latency = 0
        self.__max_latency = 0

        self.__neighbour_deps = []
        self.__total_ndeps = 0
        self.__mean_ndeps = 0
        self.__95th_percent_ndeps = 0
        self.__99th_percent_ndeps = 0
        self.__min_ndeps = 0
        self.__max_ndeps = 0

        self.__total_operations = 0

    def process(self):
        for input_path in self.__paths:
            files = [join(input_path, f) for f in listdir(input_path) if isfile(join(input_path, f))]
            for file_name in files:
                with open(file_name, 'r') as f:
                    try:
                        data = json.loads(f.read())
                    except Exception as e:
                        print "Error in %s" % file_name
                        print e

                    sizes = data["Values"]
                    samples_loaded = len(sizes)
                    recorded_samples = int(data["Samples"])
                    if samples_loaded != recorded_samples:
                        print "sample count mismatch in file %s -- %s vs %s" % (file_name, samples_loaded, recorded_samples)

                    self.__latencies.extend([op[0] for op in sizes])
                    self.__neighbour_deps.extend([op[1] for op in sizes])

        self.__latencies.sort()

        # combined sizes

        all_sizes_loaded = len(self.__latencies)
        if all_sizes_loaded != self.__total_operations:
            self.__total_operations = all_sizes_loaded
            print "sample count mismatch in -- %s vs %s" % (all_sizes_loaded, self.__total_operations)

        if len(self.__latencies) == 0:
            return False

        for latency in self.__latencies:
            self.__total_latency += latency
        self.__mean_latency = self.__total_latency / float(self.__total_operations)

        self.__95th_percent_latency = self.__latencies[int(floor(self.__total_operations * 0.95))]
        self.__99th_percent_latency = self.__latencies[int(floor(self.__total_operations * 0.99))]
        self.__min_latency = self.__latencies[0]
        self.__max_latency = self.__latencies[self.__total_operations - 1]

        assert len(self.__neighbour_deps) > 0

        for ndeps in self.__neighbour_deps:
            self.__total_ndeps += ndeps
        self.__mean_ndeps = self.__total_ndeps / float(self.__total_operations)

        self.__95th_percent_ndeps = self.__neighbour_deps[int(floor(self.__total_operations * 0.95))]
        self.__99th_percent_ndeps = self.__neighbour_deps[int(floor(self.__total_operations * 0.99))]
        self.__min_ndeps = self.__neighbour_deps[0]
        self.__max_ndeps = self.__neighbour_deps[self.__total_operations - 1]

        return True

    def plot_all_latency(self, title=None, file_path=None):
        plt.plot(self.__latencies[:int(floor(self.__total_operations))])
        plt.ylabel('seconds')
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

        data["total_operations"] = self.__total_operations

        return json.dumps(data)


    def __str__(self):
        result = ""

        result += "-------------------\n"
        result += "Total Operations Measured: %s\n" % self.__total_operations
        result += "Mean latency: %s\n" % self.__mean_latency
        result += "Min latency: %s\n" % self.__min_latency
        result += "Max latency: %s\n" % self.__max_latency
        result += "95th %% latency: %s\n" % self.__95th_percent_latency
        result += "99th %% latency: %s\n" % self.__99th_percent_latency

        result += "-------------------\n"
        return result


def local():
    paths = [
        #'/Users/kimbleoperations/Desktop/stuff/uni/mscp/project/causal_clients/scripts/ec/client/test/logs/server/delays/',
        #'/Users/kimbleoperations/Desktop/stuff/uni/mscp/project/causal_clients/scripts/orbe/client/test/logs/server/delays/',
        #'/Users/kimbleoperations/Desktop/stuff/uni/mscp/project/causal_clients/scripts/orbe_delay/client/test/logs/server/delays/',
        #'/Users/kimbleoperations/Desktop/stuff/uni/mscp/project/causal_clients/scripts/bolt_on/client/test/logs/server/delays/'

        # ec
        #base_path + '/first/ec/client/exp1405307207/logs/server/delays/',
        #base_path + '/first/ec/client/exp1405307208/logs/server/delays/',
        #base_path + '/first/ec/client/exp1405307209/logs/server/delays/',
        #base_path + '/first/ec/client/exp1405307210/logs/server/delays/',

        # orbe
        #base_path + '/first/orbe/client/exp1405338966/logs/server/delays/',
        #base_path + '/first/orbe/client/exp1405338967/logs/server/delays/',
        #base_path + '/first/orbe/client/exp1405338968/logs/server/delays/',
        #base_path + '/first/orbe/client/exp1405338969/logs/server/delays/',

        # orbe_delay
        #base_path + '/first/orbedelay/client/exp1405339774/logs/server/delays/',
        #base_path + '/first/orbedelay/client/exp1405339775/logs/server/delays/',
        #base_path + '/first/orbedelay/client/exp1405339776/logs/server/delays/',
        #base_path + '/first/orbedelay/client/exp1405339777/logs/server/delays/',

        # bolt_on
        #base_path + '/first/bolton/client/exp1405343998/logs/server/delays/',
        #base_path + '/first/bolton/client/exp1405343998/logs/server/delays/',
        #base_path + '/first/bolton/client/exp1405343998/logs/server/delays/',
        #base_path + '/first/bolton/client/exp1405343998/logs/server/delays/',
    ]

    reader = DelayStatsReader(paths)
    reader.process()
    print reader


def generate_scalability_graph(base_result_path, protocols, num_partitions, should_preserve_result_in_figure):

    results = {}
    nice_names = {"ec":"EC","bolt_on":"Bolt-on", "orbe":"Orbe", "orbe_delayed":"Orbe+"}
    colours = ["b", "g", "r", "m"]#, "c"]
    symbols = ["p", "^", "s", "D"]#, "*"]
    s = 0

    for protocol in protocols:
        partitions_to_mean_latency = []
        partitions_to_max_latency = []
        for p in range(1, num_partitions):
            partitions = p + 1

            output_path = base_result_path + "/%s/%s/" % (protocol, partitions)
            with open(output_path + "%s_delay.json" % partitions, 'r') as f:
                data = json.loads(f.read())

            partitions_to_mean_latency.append(data["mean_latency"])
            partitions_to_max_latency.append(data["max_latency"])
            results[protocol] = partitions_to_mean_latency

    plot_res = []
    for k,v in results.items():
        c = colours[s]
        partition_range = range(2, num_partitions + 1)
        plot_res.append(plt.plot(partition_range, v, c + symbols[s])[0])
        plt.plot(partition_range, v, c+"--")
        s+=1
        plt.xlabel('Number of partitions')
        plt.xticks(partition_range)
        plt.ylabel('Seconds')
        plt.title("%s protocol scaling" % protocol)

        nice_names_result = [nice_names[r] for r in results.keys()]
        plt.legend(plot_res, nice_names_result, loc=2)

        if not should_preserve_result_in_figure:
            print "Dumping " + base_result_path + "/%s/scaling_delay_%sp.pdf" % (k, num_partitions)
            plt.savefig(base_result_path + "/%s/scaling_delay_%sp.pdf" % (k, num_partitions), bbox_inches='tight')
            plt.clf()
        else:

            plt.title("Protocol scaling comparison")
            plt.savefig(base_result_path + "/comparison_delay_%sp.pdf" % num_partitions, bbox_inches='tight')


def real(num_partitions, generate_base_stats, generate_scalability, generate_comparison):
    base_input_path = '/Users/kimbleoperations/Desktop/stuff/uni/phd/logs'
    base_result_path = '/Users/kimbleoperations/Desktop/stuff/uni/phd/logs'

    #protocols = ["orbe_delayed", "orbe", "ec", "bolt_on"]
    protocols = ["orbe_delayed", "orbe"]
    #protocols = ["orbe"]

    if generate_base_stats:
        for partitions in range(1, num_partitions+1):

            for protocol in protocols:
                paths = glob.glob(base_input_path + '/%s/%s/logs/*/server/delay/' % (protocol, partitions))
                
                reader = DelayStatsReader(paths)
                if not reader.process():
                    continue

                output_path = base_result_path + "/%s/%s/" % (protocol, partitions)
                try:
                    makedirs(output_path)
                except:
                    pass

                with open(output_path + "%s_delay.json" % partitions, 'w') as f:
                    f.write(reader.to_json())

                reader.plot_all_latency("Protocol %s, %s partitions -- All latency" % (protocol, partitions), output_path + "all_delay.pdf")

    if generate_scalability:
        generate_scalability_graph(base_result_path, protocols, num_partitions, False)

    if generate_comparison:
        generate_scalability_graph(base_result_path, protocols, num_partitions, True)


if __name__ == '__main__':
    #
    real(8, True, True, True)
    #real(5, False, True, True)






