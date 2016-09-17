//
//  BoltOnCombinedRunner.cpp
//  SimRunner
//
//  Created by Scott on 18/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

//#define RUN_AS_BOLT_ON_COMBINED
#ifdef RUN_AS_BOLT_ON_COMBINED

#include <iostream>
#include <memory>
#include <vector>
#include "SimRunnerAssert.h"
#include "StandardErrorLogger.h"
#include "UniformDistribution.h"
#include "KeyValueStorage.h"
#include "KeyToPartitionMapper.h"
#include "StringHashProvider.h"
#include "PartitionReplicaBroadcaster.h"
#include "Host.h"
#include "ECPartitionServer.h"
#include "ECClient.h"
#include "ECSimulation.h"
#include "ECSerializer.h"
#include "ECGetReply.h"
#include "ECPutReply.h"
#include "ECItem.h"
#include "ECReplicateMessage.h"
#include "ECItemData.h"
#include "ECServerExchange.h"
#include "ECServerStorage.h"
#include "ECPartitionConnection.h"
#include "ECPartitionConnectionFactory.h"
#include "ECReplicaConnection.h"
#include "ECReplicaConnectionFactory.h"
#include "ConnectionResolver.h"
#include "ConnectionListener.h"
#include "Connection.h"
#include "ServerConfig.h"
#include "ServerConfigParser.h"
#include "BoltOnClientToPartitionConnection.h"
#include "BoltOnClientToPartitionConnectionFactory.h"
#include "ClientConfig.h"
#include "ClientConfigParser.h"
#include "ClientRequestGenerator.h"
#include "BoltOnClientConnection.h"
#include "BoltOnClientConnectionFactory.h"
#include "AsynchronousReadShimBackEnd.h"
#include "AsynchronousResolver.h"
#include "BoltOn.h"
#include "BoltOnClient.h"
#include "BoltOnSerializer.h"
#include "BoltOnECBackingStorage.h"
#include "BoltOnSimulation.h"
#include "BoltOnVersionApplier.h"
#include "CausalClock.h"
#include "CausalRelationship.h"
#include "KeyDependencies.h"
#include "ValueWrapperData.h"
#include "ShimDeployment.h"
#include "ValueWrapper.h"
#include "ValueWrapperFactory.h"
#include "ConnectionStats.h"
#include "MetricsStatsWriter.h"

template <typename TProtocolTraits>
class SimpleCounter
{
    typedef typename TProtocolTraits::TCounterValue TCounterValue;

public:
    SimpleCounter(TCounterValue value)
    : m_value(value)
    {

    }

    void SetValue(TCounterValue value)
    {
        m_value = value;
    }

    TCounterValue Value() const
    {
        return m_value;
    }

    TCounterValue IncrementAndGet()
    {
        return ++ m_value;
    }

private:
    TCounterValue m_value;
};

class DummyTaskPool
{
public:
    void Schedule(boost::function<void ()>& task)
    {
        task();
    }
};

class NullLogger
{
public:
    void Log(const char* format, ...) { }
};

namespace
{
    // base utils
    using namespace SimRunner::Utilities;
    typedef int TMetrics;
    typedef int TDelayTracking;
    //typedef StandardErrorLogger TLogger;
    typedef NullLogger TLogger;
    typedef NumericStringHashProvider TKeyHasher;

    // key utils
    using namespace SimRunner::Keys;
    typedef SimRunner::Keys::KeyType TStorageKey;
    typedef KeyToPartitionMapper<TStorageKey, TKeyHasher> TKeyToPartitionMapper;

    // ec protocol
    using namespace SimRunner::Protocols::EC;
    using namespace SimRunner::Protocols::EC::Messaging;
    using namespace SimRunner::Protocols::EC::Deployment::Server::Devices;

    // bolt-on
    using namespace SimRunner::Protocols;
    using namespace SimRunner::Protocols::BoltOn;
    using namespace SimRunner::Protocols::BoltOn::Deployment::Server::Devices;

    // components
    using namespace SimRunner::Components;

    using namespace SimRunner::Protocols::EC::Deployment::Server::Neighbours;
    using namespace SimRunner::Protocols::EC::Deployment::Server::Replica;
    using namespace SimRunner::Protocols::BoltOn::Deployment::Server::Client;

    using namespace SimRunner::Protocols::BoltOn::Deployment::Client;

    // metrics
    using namespace SimRunner::Metrics;
    using namespace SimRunner::Metrics::Connection;

    typedef std::string TClientInputValueType;
    typedef ECItemData<TStorageKey, TClientInputValueType> TSerializedDataType;
    typedef ECItem<TStorageKey, TClientInputValueType> TDeserializedValueType;
    typedef ECGetReplySimulation<TClientInputValueType> TClientGetReplyValueType;
    typedef ECPutReplySimulation TClientPutReplyValueType;
    typedef ECReplicateMessageSimulation<TStorageKey, TClientInputValueType, TDeserializedValueType> TReplicationMessageType;

    typedef KeyValueStorage<TStorageKey, TSerializedDataType> TStorage;

    struct ProtocolTraits
    {
        static const size_t NumPartitions = SIM_RUNNER_NUM_PARTITIONS;
        static const size_t NumReplicas = SIM_RUNNER_NUM_REPLICAS;

        typedef ECGetReply<TClientInputValueType> TClientGetReplyValueType;
        typedef ECPutReply TClientPutReplyValueType;
        typedef ECReplicateMessage<TStorageKey, TClientInputValueType, TDeserializedValueType> TReplicationMessageType;

        typedef BoltOnECBackingStorage<ProtocolTraits> TBackingStorage;

        typedef TStorageKey TKeyType;
        typedef TClientInputValueType TClientInputValueType;
        typedef TLogger TLogger;
        typedef TMetrics TMetrics;
        typedef TKeyToPartitionMapper TKeyToPartitionMapper;

        typedef MetricsStatsWriter<ProtocolTraits> TMetricsStatsWriter;
        typedef ConnectionStats<ProtocolTraits> TConnectionStats;

        typedef ECPartitionConnection<ProtocolTraits> TPartitionConnection;
        typedef ECPartitionConnectionFactory<ProtocolTraits> TPartitionConnectionFactory;
        typedef ConnectionListener<TPartitionConnectionFactory> TPartitionConnectionListener;
        typedef ConnectionResolver<ProtocolTraits, TPartitionConnectionFactory> TPartitionConnector;

        typedef ECReplicaConnection<ProtocolTraits> TReplicaConnection;
        typedef ECReplicaConnectionFactory<ProtocolTraits> TReplicaConnectionFactory;
        typedef ConnectionListener<TReplicaConnectionFactory> TReplicaConnectionListener;
        typedef ConnectionResolver<ProtocolTraits, TReplicaConnectionFactory> TReplicaConnector;

        typedef BoltOnClientConnectionFactoryFactory<ProtocolTraits> TClientConnectionFactoryFactory;
        typedef BoltOnClientConnection<ProtocolTraits> TClientConnection;
        typedef BoltOnClientConnectionFactory<ProtocolTraits> TClientConnectionFactory;
        typedef ConnectionListener<TClientConnectionFactory> TClientConnectionListener;
        typedef ConnectionResolver<ProtocolTraits, TClientConnectionFactory> TClientConnector;
        typedef TClientConnection TClientQueryResponseHandler;

        typedef ECSerializer<TSerializedDataType, TDeserializedValueType> TSerializer;

        typedef ECServerStorage<
        TStorageKey,
        TSerializedDataType,
        ProtocolTraits> TStorageComponent;

        typedef ECServerExchange<
        TStorageKey,
        TSerializedDataType,
        TDeserializedValueType,
        TClientInputValueType,
        TClientGetReplyValueType,
        TClientPutReplyValueType,
        TReplicationMessageType,
        ProtocolTraits> TNetworkExchange;

        typedef ECPartitionServer<
        TStorageKey,
        TSerializedDataType,
        TDeserializedValueType,
        TClientInputValueType,
        TClientGetReplyValueType,
        TClientPutReplyValueType,
        TReplicationMessageType,
        TStorageComponent,
        TMetrics,
        TDelayTracking,
        TLogger,
        ProtocolTraits> TPartitionServer;

        typedef PartitionReplicaBroadcaster<TNetworkExchange, TMetrics> TBroadcaster;

        typedef std::mt19937 TRandomEngine;
        typedef Distributions::UniformDistribution<TRandomEngine> TKeyDistribution;
        typedef ClientRequestGenerator<ProtocolTraits> TClientRequestGenerator;

        typedef BoltOnClientToPartitionConnection<ProtocolTraits> TConnection;
        typedef BoltOnClientToPartitionConnectionFactory<ProtocolTraits> TConnectionFactory;
        typedef ConnectionResolver<ProtocolTraits, TConnectionFactory> TConnector;

        typedef size_t TShimId;
        typedef size_t TCounterValue;
        typedef SimpleCounter<ProtocolTraits> TCounter;
        typedef std::map<TShimId, TCounter> TShimIdToWritesMapping;

        typedef TStorageKey TStorageKey;
        typedef CausalClock<ProtocolTraits> TCausalClock;
        typedef AsynchronousResolver<ProtocolTraits> TLocalStoreResolver;

        typedef ValueWrapper<ProtocolTraits> TValueWrapper;
        typedef ValueWrapperFactory<ProtocolTraits> TValueWrapperFactory;
        typedef std::shared_ptr<TValueWrapper> TValueWrapperPtr;
        typedef KeyValueStorage<TStorageKey, TValueWrapperPtr> TLocalShimStorage;

        typedef KeyDependencies<ProtocolTraits> TKeyDependencies;
        typedef AsynchronousReadShimBackEnd<ProtocolTraits> TShimBackEnd;
        typedef std::map<TStorageKey, TCausalClock> TKeyDependenciesMapping;
        typedef ShimDeployment<ProtocolTraits> TShimDeployment;
        typedef DummyTaskPool TTaskRunner;
        typedef BoltOnVersionApplier<ProtocolTraits> TVersionApplier;

        typedef KeyValueStorage<TStorageKey, TSerializedDataType> TStorage;

        typedef int32_t TItemDependencyTimestamp;

        typedef BoltOnSerializer<ProtocolTraits> TBoltOnSerializer;
        typedef ValueWrapperData<ProtocolTraits> TValueWrapperData;
        typedef ValueWrapperDataFactory<ProtocolTraits> TValueWrapperDataFactory;
        typedef std::shared_ptr<TValueWrapperData> TBoltOnSerializedValueTypePtr;

        typedef int32_t TLogicalTimestamp;

        typedef TBackingStorage TClient;
        typedef BoltOnClient<ProtocolTraits> TBoltOnClient;

    };
}

bool IsClient(int argc, const char * argv[], const char * arge[])
{
    // Declare the supported options.
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()
    ("client", "Signifies is client")
    ;

    po::variables_map vm;
    po::parsed_options parsed = po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
    po::store(parsed, vm);
    po::notify(vm);

    if (vm.count("client"))
    {
        printf("client -- bolt_on\n");
        return true;
    }
    else
    {
        printf("server -- bolt_on\n");
        return false;
    }
}

int main(int argc, const char * argv[], const char * arge[])
{
    if(IsClient(argc, argv, arge))
    {
        namespace Configuration = SimRunner::Utilities::StandardConfig::Client;
        Configuration::Config config(Configuration::ParseConfig(argc, argv, arge));

        if(!config.Valid())
        {
            return 1;
        }

        TLogger logger;
        ProtocolTraits::TConnector connector(logger);

        const size_t keySpaceSize = 100;
        std::random_device randomDevice;
        std::random_device::result_type randomEngineSeed = randomDevice();
        ProtocolTraits::TRandomEngine randomEngine(randomEngineSeed);
        std::unique_ptr<ProtocolTraits::TKeyDistribution> pKeyDistribution = ProtocolTraits::TKeyDistribution::FromUpperBound(randomEngine, keySpaceSize);
        ProtocolTraits::TClientRequestGenerator clientRequestGenerator(*pKeyDistribution);

        try
        {
            boost::asio::io_service ioService;
            ProtocolTraits::TConnectionFactory connectionFactory(ioService, randomEngine, clientRequestGenerator, logger, config.Host());

            for(size_t i = 0; i < config.Connections(); ++ i)
            {
                connector.TryConnectSynchronous(connectionFactory,
                                                ioService,
                                                config.Host(),
                                                config.Port());
            }

            ioService.run();
        }
        catch (std::exception& e)
        {
            std::cerr << "Exception: " << e.what() << "\n";
        }
    }
    else
    {
        namespace Configuration = SimRunner::Utilities::StandardConfig::Server;
        Configuration::Config config(Configuration::ParseConfig(argc, argv, arge));

        if(!config.Valid())
        {
            return 1;
        }

        const SimRunner::Utilities::StandardConfig::ServerNetworkTopologyModel& serverNetworkTopologyModel(config.GetServerNetworkTopologyModel());

        TDelayTracking delayTracking(0);
        TMetrics metrics(0);
        TLogger logger;

        logger.Log("Partitions count: %d\n", ProtocolTraits::NumPartitions);
        logger.Log("Replica count: %d\n", ProtocolTraits::NumReplicas);
        logger.Log("This partition: %d\n", config.PartitionId());
        logger.Log("This replica: %d\n", config.ReplicaId());

        std::vector<Host> replicaHosts(serverNetworkTopologyModel.ReplicatedPartitions());
        std::vector<Host> partitionHosts(serverNetworkTopologyModel.PartitionNeighbours());
        const SimRunner::Protocols::ReplicaIdType replicaId(config.ReplicaId());
        const SimRunner::Protocols::PartitionIdType partitionId(config.PartitionId());

        SR_ASSERT(replicaHosts.size() == ProtocolTraits::NumReplicas, "Mismatched replicas: got %d hosts, compiled for %d.",
                  replicaHosts.size(),
                  ProtocolTraits::NumReplicas);

        SR_ASSERT(partitionHosts.size() == ProtocolTraits::NumPartitions, "Mismatched partitions: got %d hosts, compiled for %d.",
                  partitionHosts.size(),
                  ProtocolTraits::NumReplicas);

        TKeyHasher hasher;
        TKeyToPartitionMapper keyToPartitionMapper(partitionHosts.size(), hasher);

        uint16_t partitionListenPort(boost::lexical_cast<uint16_t>(serverNetworkTopologyModel.PartitionPort()));
        uint16_t replicaListenPort(boost::lexical_cast<uint16_t>(serverNetworkTopologyModel.ReplicaPort()));
        uint16_t clientListenPort(boost::lexical_cast<uint16_t>(serverNetworkTopologyModel.ClientPort()));
        const bool isLocalTestRun(serverNetworkTopologyModel.LocalTestRun());

        ProtocolTraits::TShimId shimId((replicaId * 10000) + partitionId);

        ProtocolTraits::TPartitionConnector partitionConnector(logger);
        ProtocolTraits::TReplicaConnector replicaConnector(logger);

        ProtocolTraits::TTaskRunner taskRunner;
        ProtocolTraits::TValueWrapperDataFactory valueWrapperDataFactory;
        ProtocolTraits::TValueWrapperFactory valueWrapperFactory(valueWrapperDataFactory);
        ProtocolTraits::TBoltOnSerializer boltOnSerializer(valueWrapperFactory);

        ProtocolTraits::TClientConnectionFactoryFactory boltOnClientConnectionFactoryFactory(keyToPartitionMapper,
                                                                                             valueWrapperDataFactory,
                                                                                             metrics,
                                                                                             logger);

        ProtocolTraits::TNetworkExchange networkExchange(partitionId,
                                                         replicaId,
                                                         partitionListenPort,
                                                         replicaListenPort,
                                                         clientListenPort,
                                                         partitionHosts,
                                                         replicaHosts,
                                                         keyToPartitionMapper,
                                                         partitionConnector,
                                                         replicaConnector,
                                                         boltOnClientConnectionFactoryFactory,
                                                         metrics,
                                                         logger,
                                                         isLocalTestRun);

        ProtocolTraits::TSerializer serializer;

        typedef std::unique_ptr<TStorage> TStoragePtr;
        typedef std::unique_ptr<ProtocolTraits::TStorageComponent> TStorageComponentPtr;
        typedef std::unique_ptr<ProtocolTraits::TBroadcaster> TBroadcasterPtr;
        typedef std::unique_ptr<ProtocolTraits::TPartitionServer> TPartitionServerPtr;

        TStoragePtr storage(new TStorage());

        TStorageComponentPtr storageComponent(new ProtocolTraits::TStorageComponent(*storage));

        TBroadcasterPtr broadcaster(new ProtocolTraits::TBroadcaster(replicaId, partitionId, networkExchange, metrics));

        TPartitionServerPtr partitionServer(new ProtocolTraits::TPartitionServer(replicaId,
                                                                                 partitionId,
                                                                                 *storageComponent,
                                                                                 networkExchange,
                                                                                 *broadcaster,
                                                                                 serializer,
                                                                                 metrics,
                                                                                 delayTracking,
                                                                                 logger));

        typedef std::unique_ptr<ProtocolTraits::TLocalShimStorage> TBoltOnStoragePtr;
        TBoltOnStoragePtr localShimStorage(new ProtocolTraits::TLocalShimStorage());

        typedef std::unique_ptr<ProtocolTraits::TBackingStorage> TBackingStoragePtr;
        TBackingStoragePtr backingStorageComponentPtr(new ProtocolTraits::TBackingStorage(-1,
                                                                                          keyToPartitionMapper,
                                                                                          *partitionServer,
                                                                                          networkExchange,
                                                                                          valueWrapperDataFactory));

        ProtocolTraits::TLocalStoreResolver resolver(*backingStorageComponentPtr, *localShimStorage, taskRunner, boltOnSerializer, valueWrapperFactory);
        ProtocolTraits::TShimBackEnd shimBackEnd(shimId, *localShimStorage, resolver, boltOnSerializer, valueWrapperFactory);

        ProtocolTraits::TShimDeployment shimDeployment(shimId,
                                                       resolver,
                                                       shimBackEnd);

        boltOnClientConnectionFactoryFactory.SetShim(shimDeployment);

        try
        {
            boost::asio::io_service ioService;

            networkExchange.Bootstrap(*partitionServer,
                                      *broadcaster,
                                      ioService);

            ioService.run();
        }
        catch (std::exception& e)
        {
            std::cerr << "Exception: " << e.what() << "\n";
        }
    }

    return 0;
}

#endif
