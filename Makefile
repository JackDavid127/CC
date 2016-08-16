#
# make simrunner
#
# Local: Assumes clang++ >= 3.5.0 and boost >= 1_56
# Uni: Assumes g++ >= 4.8.2 and boost >= 1_56
#

# define the C compiler to use
CC = g++

# define any compile-time flags
CXXFLAGS = -Wall -O3 -std=c++11 -D$(RUNNER) -D$(SIM_RUNNER_NUM_PARTITIONS) -D$(SIM_RUNNER_NUM_REPLICAS)
CXXFLAGS += -fpermissive

ROOT = $(PWD)

# define any directories containing header files other than /usr/include
#
INCLUDES = \
	$(BOOST_INCLUDES) \
	-I/$(ROOT)/src \
	-I/$(ROOT)/src/components \
	-I/$(ROOT)/src/keys \
	-I/$(ROOT)/src/keys/distributions \
	-I/$(ROOT)/src/metrics \
	-I/$(ROOT)/src/metrics/connection \
	-I/$(ROOT)/src/metrics/delay \
	-I/$(ROOT)/src/metrics/metadata \
	-I/$(ROOT)/src/protocols \
	-I/$(ROOT)/src/protocols/bolt_on \
	-I/$(ROOT)/src/protocols/bolt_on/deployment \
	-I/$(ROOT)/src/protocols/bolt_on/deployment/client \
	-I/$(ROOT)/src/protocols/bolt_on/deployment/server \
	-I/$(ROOT)/src/protocols/bolt_on/deployment/server/client \
	-I/$(ROOT)/src/protocols/bolt_on/deployment/server/devices \
	-I/$(ROOT)/src/protocols/bolt_on/deployment/wire \
	-I/$(ROOT)/src/protocols/bolt_on/simulated_components \
	-I/$(ROOT)/src/protocols/bolt_on/version_applier_states \
	-I/$(ROOT)/src/protocols/ec \
	-I/$(ROOT)/src/protocols/ec/deployment \
	-I/$(ROOT)/src/protocols/ec/deployment/client \
	-I/$(ROOT)/src/protocols/ec/deployment/server \
	-I/$(ROOT)/src/protocols/ec/deployment/server/client \
	-I/$(ROOT)/src/protocols/ec/deployment/server/devices \
	-I/$(ROOT)/src/protocols/ec/deployment/server/neighbours \
	-I/$(ROOT)/src/protocols/ec/deployment/server/replicas \
	-I/$(ROOT)/src/protocols/ec/deployment/wire \
	-I/$(ROOT)/src/protocols/ec/messaging \
	-I/$(ROOT)/src/protocols/ec/simulated_components \
	-I/$(ROOT)/src/protocols/orbe \
	-I/$(ROOT)/src/protocols/orbe/deployment \
	-I/$(ROOT)/src/protocols/orbe/deployment/client \
	-I/$(ROOT)/src/protocols/orbe/deployment/server \
	-I/$(ROOT)/src/protocols/orbe/deployment/server/client \
	-I/$(ROOT)/src/protocols/orbe/deployment/interactive \
	-I/$(ROOT)/src/protocols/orbe/deployment/server/devices \
	-I/$(ROOT)/src/protocols/orbe/deployment/server/neighbours \
	-I/$(ROOT)/src/protocols/orbe/deployment/server/replicas \
	-I/$(ROOT)/src/protocols/orbe/deployment/wire \
	-I/$(ROOT)/src/protocols/orbe/messaging \
	-I/$(ROOT)/src/protocols/orbe/simulated_components \
	-I/$(ROOT)/src/protocols/orbe_delayed_install \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/deployment \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/deployment/client \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/deployment/interactive \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/deployment/server \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/deployment/server/client \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/deployment/server/devices \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/deployment/server/neighbours \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/deployment/server/replicas \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/deployment/wire \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/messaging \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/simulator \
	-I/$(ROOT)/src/protocols/orbe_delayed_install/simulator/simulated_components \
	-I/$(ROOT)/src/simulation_infrastructure \
	-I/$(ROOT)/src/utilities \
	-I/$(ROOT)/src/utilities/standard_config \
	-I/$(ROOT)/src/utilities/standard_config/client \
	-I/$(ROOT)/src/utilities/standard_config/server \
	\
	-I/$(ROOT)/lib/rapidjson \
	-I/$(ROOT)/lib/rapidjson/internal \


# define library paths in addition to /usr/lib
LFLAGS = $(BOOST_LIBS) \

# define any libraries to link into executable:
LIBS = -lboost_system -lboost_chrono -lboost_program_options -lboost_filesystem

# define the source files
SRCS = \
	./src/protocols/bolt_on/BoltOnSimulationRunner.cpp \
	./src/protocols/bolt_on/deployment/BoltOnCombinedRunner.cpp \
	./src/protocols/ec/ECSimulatorRunner.cpp \
	./src/protocols/ec/deployment/ECCombinedRunner.cpp \
	./src/protocols/orbe/OrbeSimulatorRunner.cpp \
	./src/protocols/orbe/deployment/OrbeCombinedRunner.cpp \
	./src/protocols/orbe_delayed_install/simulator/OrbeDelayedInstallSimulatorRunner.cpp \
	./src/protocols/orbe_delayed_install/deployment/OrbeDelayedInstallCombinedRunner.cpp \
	./src/utilities/SimRunnerAssert.cpp \
	./src/utilities/Wire.cpp

# define the object files with suffix replacement
OBJS = $(SRCS:.cpp = .o)

# define the executable file 
MAIN = simrunner

#
# The following part of the makefile is generic;
#

OBJDIR = $(ROOT)/obj

.PHONY: clean

all:    $(MAIN)
		@echo $(MAIN) has been compiled

$(MAIN): $(OBJS) 
		$(CC) $(CXXFLAGS) $(INCLUDES) -o $(MAIN) $(OBJS) $(LFLAGS) $(LIBS)

clean:
		$(RM) *.o *~ bin/$(MAIN) 
