//
//  OrbeDelayedInstallCombinedRunner.cpp
//  SimRunner
//
//  Created by Scott on 26/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

//#define SIM_RUNNER_RUN_AS_ORBE_DELAYED_INSTALL_COMBINED
#ifdef SIM_RUNNER_RUN_AS_ORBE_DELAYED_INSTALL_COMBINED

#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <boost/lexical_cast.hpp>
#include "SimRunnerAssert.h"
#include "StandardErrorLogger.h"
#include "UniformDistribution.h"
#include "KeyValueStorage.h"
#include "KeyToPartitionMapper.h"
#include "StringHashProvider.h"
#include "PartitionReplicaBroadcaster.h"
#include "Host.h"
#include "OrbeDelayedInstallPartitionServer.h"
#include "OrbeDelayedInstallClient.h"
#include "OrbeDelayedInstallSerializer.h"
#include "OrbeDelayedInstallGetReply.h"
#include "OrbeDelayedInstallPutReply.h"
#include "OrbeDelayedInstallItem.h"
#include "OrbeDelayedInstallReplicateMessage.h"
#include "OrbeDelayedInstallItemData.h"
#include "OrbeDelayedInstallServerExchange.h"
#include "OrbeDelayedInstallServerStorage.h"
#include "OrbeDelayedInstallPartitionConnection.h"
#include "OrbeDelayedInstallPartitionConnectionFactory.h"
#include "OrbeDelayedInstallReplicaConnection.h"
#include "OrbeDelayedInstallReplicaConnectionFactory.h"
#include "OrbeDelayedInstallClientConnection.h"
#include "OrbeDelayedInstallClientConnectionFactory.h"
#include "ConnectionResolver.h"
#include "ConnectionListener.h"
#include "Connection.h"
#include "ServerConfig.h"
#include "ServerConfigParser.h"
#include "OrbeDelayedInstallClientToPartitionConnection.h"
#include "OrbeDelayedInstallClientToPartitionConnectionFactory.h"
#include "ClientConfig.h"
#include "ClientConfigParser.h"
#include "ClientRequestGenerator.h"
#include "VersionVector.h"
#include "OrbeDelayedInstallPartitionServerReplicationHandler.h"
#include "ConnectionStats.h"
#include "DelayStats.h"
#include "MetricsStatsWriter.h"
#include "OrbeDelayedInstallClientUpdateSpace.h"
#include "OrbeDelayedInstallReplicaUpdateSpace.h"

class NullLogger
{
public:
    void Log(const char* format, ...) { }
};

#include <boost/asio.hpp>
#include <functional>
#include <chrono>
#include <thread>
#include <iostream>

template<typename TProtocolTraits>
class HeartbeatTask
{
    typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
    
public:
    HeartbeatTask(boost::asio::io_service& ioService,
                  TPartitionServer& partitionServer)
    : m_runner(std::bind(&HeartbeatTask::Run, this))
    , m_strand(ioService)
   // , m_timer(ioService, boost::posix_time::seconds(1))
    , m_partitionServer(partitionServer)
    {
       // m_timer.async_wait(m_strand.wrap(boost::bind(&HeartbeatTask::PerformHeartbeat, this)));
    }
    
    ~HeartbeatTask()
    {
        m_runner.join();
    }
    
private:
    void Run()
    {
        while(true)
        {
            //std::cout << "t1's id: " << std::this_thread::get_id() << '\n';
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            m_strand.dispatch(boost::bind(&HeartbeatTask::PerformHeartbeat, this));
        }
    }
    
    void PerformHeartbeat()
    {
        //std::cout << "thread id: " << std::this_thread::get_id() << '\n';
        m_partitionServer.SendHeartbeatVector();
        //m_timer.async_wait(m_strand.wrap(boost::bind(&HeartbeatTask::PerformHeartbeat, this)));
    }
    
    std::thread m_runner;
    boost::asio::strand m_strand;
    //boost::asio::deadline_timer m_timer;
    TPartitionServer& m_partitionServer;
};

namespace
{
    using namespace SimRunner;
    
    // base utils
    using namespace SimRunner::Utilities;
    typedef int TMetrics;
    //typedef StandardErrorLogger TLogger;
    typedef NullLogger TLogger;
    typedef NumericStringHashProvider TKeyHasher;
    
    // key utils
    using namespace SimRunner::Keys;
    typedef SimRunner::Keys::KeyType TStorageKey;
    typedef KeyToPartitionMapper<TStorageKey, TKeyHasher> TKeyToPartitionMapper;
    
    // OrbeDelayedInstall protocol
    using namespace SimRunner::Protocols::OrbeDelayedInstall;
    using namespace SimRunner::Protocols::OrbeDelayedInstall::Messaging;
    using namespace SimRunner::Protocols::OrbeDelayedInstall::Deployment::Server::Devices;
    using namespace SimRunner::Protocols::OrbeDelayedInstall::Messaging;
    
    // client input value -- string or int
    // partition server item -- OrbeDelayedInstallItem containing client input + metadata
    // serialized value -- OrbeDelayedInstallItem serialized to string -- TSerializedData
    // client output value -- OrbeDelayedInstallGetReply, OrbeDelayedInstallPutReply
    typedef int TClientInputValueType;
  
    // components
    using namespace SimRunner::Components;
    
    using namespace SimRunner::Protocols;
    using namespace SimRunner::Protocols::OrbeDelayedInstall::Deployment::Server::Neighbours;
    using namespace SimRunner::Protocols::OrbeDelayedInstall::Deployment::Server::Replica;
    using namespace SimRunner::Protocols::OrbeDelayedInstall::Deployment::Server::Client;
    using namespace SimRunner::Protocols::OrbeDelayedInstall::Deployment::Client;
    
    // metrics
    using namespace SimRunner::Metrics;
    using namespace SimRunner::Metrics::Connection;
    using namespace SimRunner::Metrics::Delay;
    
    struct ProtocolTraits
    {
        static const size_t NumPartitions = SIM_RUNNER_NUM_PARTITIONS;
        static const size_t NumReplicas = SIM_RUNNER_NUM_REPLICAS;
        
        static_assert(NumPartitions > 0, "Partition count must be greater than zero.\n");
        static_assert(NumReplicas > 0, "Replica count must be greater than zero.\n");
        
        typedef TMetrics TMetrics;
        typedef TLogger TLogger;
        
        typedef MetricsStatsWriter<ProtocolTraits> TMetricsStatsWriter;
        typedef ConnectionStats<ProtocolTraits> TConnectionStats;
        typedef DelayStats<ProtocolTraits> TDelayTracking;
        
        typedef OrbeDelayedInstallItemData<ProtocolTraits> TSerializedDataType;
        typedef OrbeDelayedInstallItem<ProtocolTraits> TDeserializedValueType;
        typedef OrbeDelayedInstallGetReply<ProtocolTraits> TClientGetReplyValueType;
        typedef OrbeDelayedInstallPutReply<ProtocolTraits> TClientPutReplyValueType;
        typedef OrbeDelayedInstallReplicateMessageSimulation<ProtocolTraits> TReplicationMessageType;
        typedef KeyValueStorage<TStorageKey, TSerializedDataType> TStorage;
        
        typedef TStorageKey TKeyType;
        typedef TClientInputValueType TClientInputValueType;
        typedef TKeyToPartitionMapper TKeyToPartitionMapper;
        
        typedef OrbeDelayedInstallPartitionConnection<ProtocolTraits> TPartitionConnection;
        typedef OrbeDelayedInstallPartitionConnectionFactory<ProtocolTraits> TPartitionConnectionFactory;
        typedef ConnectionListener<TPartitionConnectionFactory> TPartitionConnectionListener;
        typedef ConnectionResolver<ProtocolTraits, TPartitionConnectionFactory> TPartitionConnector;
        
        typedef OrbeDelayedInstallReplicaConnection<ProtocolTraits> TReplicaConnection;
        typedef OrbeDelayedInstallReplicaConnectionFactory<ProtocolTraits> TReplicaConnectionFactory;
        typedef ConnectionListener<TReplicaConnectionFactory> TReplicaConnectionListener;
        typedef ConnectionResolver<ProtocolTraits, TReplicaConnectionFactory> TReplicaConnector;
        
        typedef OrbeDelayedInstallClientConnection<ProtocolTraits> TClientConnection;
        typedef OrbeDelayedInstallClientConnectionFactory<ProtocolTraits> TClientConnectionFactory;
        typedef ConnectionListener<TClientConnectionFactory> TClientConnectionListener;
        typedef ConnectionResolver<ProtocolTraits, TClientConnectionFactory> TClientConnector;
        typedef std::shared_ptr<TClientConnection> TClientQueryResponseHandlerPtr;
        
        typedef OrbeDelayedInstallSerializer<TSerializedDataType, TDeserializedValueType> TSerializer;
        
        typedef TStorageKey TStorageKey;
        typedef int32_t TItemDependencyTimestamp;
        typedef int32_t TLogicalTimestamp;
        typedef SimRunner::Utilities::VersionVector<NumReplicas, TLogicalTimestamp> TVersionVector;
        
        typedef int32_t TClientDependencyTimestamp;
        typedef PartitionIdType TClientDependencyPartition;
        typedef ReplicaIdType TItemDependencyReplica;
        typedef ClientIdType TClientId;
        
        typedef OrbeDelayedInstallClientUpdateSpace<ProtocolTraits> TClientUpdateSpace;
        typedef OrbeDelayedInstallReplicaUpdateSpace<ProtocolTraits> TReplicaUpdateSpace;
        
        typedef OrbeDelayedInstallServerStorage<
        TStorageKey,
        TSerializedDataType,
        ProtocolTraits> TStorageComponent;
        
        typedef OrbeDelayedInstallServerExchange<
        TStorageKey,
        TSerializedDataType,
        TDeserializedValueType,
        TClientInputValueType,
        TClientGetReplyValueType,
        TClientPutReplyValueType,
        TReplicationMessageType,
        ProtocolTraits> TNetworkExchange;
        
        typedef OrbeDelayedInstallPartitionServer<ProtocolTraits> TPartitionServer;
        
        typedef OrbeDelayedInstallPartitionServerReplicationHandler<ProtocolTraits> TReplicationHandler;
        
        typedef OrbeDelayedInstallClient<ProtocolTraits> TClient;
        
        typedef PartitionReplicaBroadcaster<TNetworkExchange, TMetrics> TBroadcaster;
        
        typedef TClient TStorageComponentRequestMetaData;
        
        //client
        typedef std::mt19937 TRandomEngine;
        typedef Distributions::UniformDistribution<TRandomEngine> TKeyDistribution;
        typedef ClientRequestGenerator<ProtocolTraits> TClientRequestGenerator;
        
        typedef OrbeDelayedInstallClientToPartitionConnection<ProtocolTraits> TConnection;
        typedef OrbeDelayedInstallClientToPartitionConnectionFactory<ProtocolTraits> TConnectionFactory;
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
        printf("client -- orbe+\n");
        return true;
    }
    else
    {
        printf("server -- orbe+\n");
        return false;
    }
}

int main(int argc, const char * argv[], const char * arge[])
{
    unsigned int n = std::thread::hardware_concurrency();
    std::cout << n << " concurrent threads are supported.\n";
    
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
        
        const size_t keySpaceSize = 10000000;
        std::random_device randomDevice;
        std::random_device::result_type randomEngineSeed = randomDevice();
        ProtocolTraits::TRandomEngine randomEngine(randomEngineSeed);
        std::unique_ptr<ProtocolTraits::TKeyDistribution> pKeyDistribution = ProtocolTraits::TKeyDistribution::FromUpperBound(randomEngine, keySpaceSize);
        ProtocolTraits::TClientRequestGenerator clientRequestGenerator(*pKeyDistribution);
        
        try
        {
            boost::asio::io_service ioService;
            ProtocolTraits::TConnectionFactory connectionFactory(ioService,
                                                                 randomEngine,
                                                                 clientRequestGenerator,
                                                                 logger,
                                                                 config.Host());
            
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
        
        TMetrics metrics(0);
        TLogger logger;
        
        logger.Log("Partitions count: %d\n", ProtocolTraits::NumPartitions);
        logger.Log("Replica count: %d\n", ProtocolTraits::NumReplicas);
        logger.Log("This partition: %d\n", config.PartitionId());
        logger.Log("This replica: %d\n", config.ReplicaId());
        logger.Log("This host: %s\n", config.Host().c_str());
        
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
                                                         metrics,
                                                         logger,
                                                         isLocalTestRun);
        
        ProtocolTraits::TSerializer serializer;
        
        typedef std::unique_ptr<ProtocolTraits::TStorage> TStoragePtr;
        typedef std::unique_ptr<ProtocolTraits::TStorageComponent> TStorageComponentPtr;
        typedef std::unique_ptr<ProtocolTraits::TClientUpdateSpace> TClientUpdateSpacePtr;
        typedef std::unique_ptr<ProtocolTraits::TReplicaUpdateSpace> TReplicaUpdateSpacePtr;
        typedef std::unique_ptr<ProtocolTraits::TBroadcaster> TBroadcasterPtr;
        typedef std::unique_ptr<ProtocolTraits::TPartitionServer> TPartitionServerPtr;
        
        TStoragePtr storage(new ProtocolTraits::TStorage());
        TStorageComponentPtr storageComponent(new ProtocolTraits::TStorageComponent(*storage));
        TClientUpdateSpacePtr clientUpdateSpacePtr(new ProtocolTraits::TClientUpdateSpace);
        TReplicaUpdateSpacePtr replicaUpdateSpacePtr(new ProtocolTraits::TReplicaUpdateSpace);
        
        TBroadcasterPtr broadcaster(new ProtocolTraits::TBroadcaster(replicaId, partitionId, networkExchange, metrics));
        
        ProtocolTraits::TMetricsStatsWriter delayStatsWriter("./logs/delay/" + config.Host() + "/");
        std::string serverName("p" + boost::lexical_cast<std::string>(config.PartitionId()) +
                                "_r" + boost::lexical_cast<std::string>(config.ReplicaId()));
        ProtocolTraits::TDelayTracking delayTracking(delayStatsWriter, serverName, 10);
        
        TPartitionServerPtr partitionServer(new ProtocolTraits::TPartitionServer(replicaId,
                                                                                 partitionId,
                                                                                 *storageComponent,
                                                                                 networkExchange,
                                                                                 *broadcaster,
                                                                                 serializer,
                                                                                 *clientUpdateSpacePtr,
                                                                                 *replicaUpdateSpacePtr,
                                                                                 metrics,
                                                                                 delayTracking,
                                                                                 logger));
        try
        {
            boost::asio::io_service ioService;
            
            networkExchange.Bootstrap(*partitionServer,
                                      *broadcaster,
                                      ioService);
            
            HeartbeatTask<ProtocolTraits> heartbeatTask(ioService, *partitionServer);
            
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
