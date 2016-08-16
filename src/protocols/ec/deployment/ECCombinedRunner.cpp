//
//  ECCombinedRunner.cpp
//  SimRunner
//
//  Created by Scott on 26/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

//#define RUN_AS_EC_COMBINED
#ifdef RUN_AS_EC_COMBINED

#include <iostream>
#include <memory>
#include <vector>
#include <string>
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
#include "ECClientConnection.h"
#include "ECClientConnectionFactory.h"
#include "ConnectionResolver.h"
#include "ConnectionListener.h"
#include "Connection.h"
#include "ServerConfig.h"
#include "ServerConfigParser.h"
#include "ECClientToPartitionConnection.h"
#include "ECClientToPartitionConnectionFactory.h"
#include "ClientConfig.h"
#include "ClientConfigParser.h"
#include "ClientRequestGenerator.h"
#include "ConnectionStats.h"
#include "MetricsStatsWriter.h"

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
    
    // client input value -- string or int
    // partition server item -- ECItem containing client input + metadata
    // serialized value -- ECItem serialized to string -- TSerializedData
    // client output value -- ECGetReply, ECPutReply
    typedef std::string TClientInputValueType;
    typedef ECItemData<TStorageKey, TClientInputValueType> TSerializedDataType;
    typedef ECItem<TStorageKey, TClientInputValueType> TDeserializedValueType;
    typedef ECGetReplySimulation<TClientInputValueType> TClientGetReplyValueType;
    typedef ECPutReplySimulation TClientPutReplyValueType;
    typedef ECReplicateMessageSimulation<TStorageKey, TClientInputValueType, TDeserializedValueType> TReplicationMessageType;
    
    // components
    using namespace SimRunner::Components;
    typedef KeyValueStorage<TStorageKey, TSerializedDataType> TStorage;
    
    // metrics
    using namespace SimRunner::Metrics;
    using namespace SimRunner::Metrics::Connection;
    
    using namespace SimRunner::Protocols::EC::Deployment::Server::Neighbours;
    using namespace SimRunner::Protocols::EC::Deployment::Server::Replica;
    using namespace SimRunner::Protocols::EC::Deployment::Server::Client;
    using namespace SimRunner::Protocols::EC::Deployment::Client;
    
    struct ProtocolTraits
    {
        static const size_t NumPartitions = SIM_RUNNER_NUM_PARTITIONS;
        static const size_t NumReplicas = SIM_RUNNER_NUM_REPLICAS;
        
        typedef ECGetReply<TClientInputValueType> TClientGetReplyValueType;
        typedef ECPutReply TClientPutReplyValueType;
        typedef ECReplicateMessage<TStorageKey, TClientInputValueType, TDeserializedValueType> TReplicationMessageType;
        
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
        
        typedef ECClientConnectionFactoryFactory<ProtocolTraits> TClientConnectionFactoryFactory;
        typedef ECClientConnection<ProtocolTraits> TClientConnection;
        typedef ECClientConnectionFactory<ProtocolTraits> TClientConnectionFactory;
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
        
        typedef ECClient<
        TStorageKey,
        TClientInputValueType,
        TClientGetReplyValueType,
        TClientPutReplyValueType,
        TKeyToPartitionMapper,
        TMetrics,
        TLogger,
        ProtocolTraits
        > TClient;
        
        typedef PartitionReplicaBroadcaster<TNetworkExchange, TMetrics> TBroadcaster;
        
        typedef TClient TStorageComponentRequestMetaData;
        
        //client
        typedef std::mt19937 TRandomEngine;
        typedef Distributions::UniformDistribution<TRandomEngine> TKeyDistribution;
        typedef ClientRequestGenerator<ProtocolTraits> TClientRequestGenerator;
        
        typedef ECClientToPartitionConnection<ProtocolTraits> TConnection;
        typedef ECClientToPartitionConnectionFactory<ProtocolTraits> TConnectionFactory;
        typedef ConnectionResolver<ProtocolTraits, TConnectionFactory> TConnector;
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
        printf("client -- ec\n");
        return true;
    }
    else
    {
        printf("server -- ec\n");
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
        
        ProtocolTraits::TPartitionConnector partitionConnector(logger);
        ProtocolTraits::TReplicaConnector replicaConnector(logger);
        
        ProtocolTraits::TClientConnectionFactoryFactory clientConnectionFactoryFactory(keyToPartitionMapper, metrics, logger);
        
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
                                                         clientConnectionFactoryFactory,
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
