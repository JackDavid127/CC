#! /usr/bin/env sh

usage() { echo "Usage: $0 -p (partitions) -r (replicas) [-s]"; 1>&2; exit 1; }

pushd . > /dev/null
cd ${0%/*}
cd ..

runner=RUN_AS_EC_COMBINED
replica=
partition=

while getopts ":p:r:s" opt; do
  case $opt in
    s)
		runner=RUN_AS_EC_SIMULATOR
      ;;
    r)
		replica=${OPTARG}
      ;;
    p)
		partition=${OPTARG}
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

if [ -z $runner ] || [ -z $replica ] || [ -z $partition ]; then
	usage
fi

if ! [ $replica -eq $replica 2> /dev/null ]; then
	echo "Error: Replica is $replica, must be a number."
	usage
fi

if ! [ $partition -eq $partition 2> /dev/null ]; then
	echo "Error: Partition is $partition, must be a number."
	usage
fi

make clean
make RUNNER=$runner SIM_RUNNER_NUM_REPLICAS="SIM_RUNNER_NUM_REPLICAS=$replica" SIM_RUNNER_NUM_PARTITIONS="SIM_RUNNER_NUM_PARTITIONS=$partition"
mkdir -p ./bin
rm -f ./bin/simrunner
mv ./simrunner ./bin/simrunner

popd > /dev/null
