//
//  main.cpp
//  SimRunner
//
//  Created by Scott on 24/09/2014.
//
//

//#define RUN_AS_EC_SIMULATOR
#ifdef RUN_AS_EC_SIMULATOR

#include <iostream>
#include <memory>
#include <string>
#include "SimRunnerAssert.h"
#include "StandardErrorLogger.h"
#include "SimulationClock.h"
#include "SimulationEventSchedule.h"
#include "SimulationHost.h"
#include "UniformDistribution.h"
#include "KeyGenerator.h"
#include "SimulatedStorageComponent.h"
#include "SimulatedNetworkLinkComponent.h"
#include "KeyToPartitionMapper.h"
#include "StringHashProvider.h"
#include "PartitionReplicaBroadcaster.h"
#include "ECPartitionServer.h"
#include "ECSimulatedExchange.h"
#include "ECClient.h"
#include "ECSimulation.h"
#include "ECSerializer.h"
#include "ECGetReply.h"
#include "ECPutReply.h"
#include "ECItem.h"
#include "ECReplicateMessage.h"
#include "ECItemData.h"

namespace
{
    typedef std::mt19937 TRandomEngine;
    
    // base utils
    using namespace SimRunner::Utilities;
    typedef int TMetrics;
    typedef int TDelayTracking;
    typedef StandardErrorLogger TLogger;
    typedef NumericStringHashProvider TKeyHasher;
    
    // key utils
    using namespace SimRunner::Keys;
    typedef SimRunner::Keys::KeyType TStorageKey;
    typedef Distributions::UniformDistribution<TRandomEngine> TKeyDistribution;
    typedef KeyGenerator<TKeyDistribution> TKeyGenerator;
    typedef KeyToPartitionMapper<TStorageKey, TKeyHasher> TKeyToPartitionMapper;
    
    // ec protocol
    using namespace SimRunner::Protocols::EC;
    using namespace SimRunner::Protocols::EC::Messaging;
    using namespace SimRunner::Protocols::EC::SimulatedComponents;
    
    // client input value -- string or int
    // partition server item -- ECItem containing client input + metadata
    // serialized value -- ECItem serialized to string -- TSerializedData
    // client output value -- ECGetReply, ECPutReply
    
    // base simulation
    using namespace SimRunner::SimulationInfrastructure;
    typedef SimulationClock TSimulationClock;
    typedef SimulationEventSchedule<TSimulationClock, TLogger> TEventSchedule;
    
    // components
    using namespace SimRunner::Components;
    
    struct ProtocolTraits
    {
        typedef TEventSchedule TEventSchedule;
        typedef TMetrics TMetrics;
        typedef TMetrics TDelayTracking;
        typedef TLogger TLogger;
        
        typedef std::string TClientInputValueType;
        typedef ECItemData<TStorageKey, TClientInputValueType> TSerializedDataType;
        typedef ECItem<TStorageKey, TClientInputValueType> TDeserializedValueType;
        typedef ECGetReplySimulation<TClientInputValueType> TClientGetReplyValueType;
        typedef ECPutReplySimulation TClientPutReplyValueType;
        typedef ECReplicateMessageSimulation<TStorageKey, TClientInputValueType, TDeserializedValueType> TReplicationMessageType;
        
        typedef KeyValueStorage<TStorageKey, TSerializedDataType> TStorage;
        
        typedef TStorageKey TStorageKey;
        
        
        typedef ECSerializer<TSerializedDataType, TDeserializedValueType> TSerializer;
        
        typedef SimulatedStorageComponent<ProtocolTraits> TStorageComponent;
        
        typedef SimulatedNetworkLinkComponent<TEventSchedule> TNetworkLink;
        
        typedef ECSimulatedExchange<
        TEventSchedule,
        TMetrics,
        TLogger,
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
        
        typedef ECSimulation<
        TStorage,
        TEventSchedule,
        TKeyGenerator,
        TKeyToPartitionMapper,
        TRandomEngine,
        TLogger,
        TMetrics,
        TDelayTracking,
        TClientInputValueType,
        TClientGetReplyValueType,
        TClientPutReplyValueType,
        ProtocolTraits> TProtocol;
        
        typedef TProtocol TClientQueryResponseHandler;
    };
    
    // simulation runner
    typedef SimRunner::SimulationInfrastructure::SimulationHost<ProtocolTraits::TProtocol, TEventSchedule, TLogger> TSimulationHost;
}

int main(int argc, const char * argv[], const char * arge[])
{
    const size_t keySpaceSize = 100;
    const std::random_device::result_type deterministicSeed = 12345;
    const bool deterministic = true;
    
    std::random_device randomDevice;
    std::random_device::result_type randomEngineSeed = deterministic ? deterministicSeed : randomDevice();
    TRandomEngine randomEngine(randomEngineSeed);
    std::unique_ptr<TKeyDistribution> pKeyDistribution = TKeyDistribution::FromUpperBound(randomEngine, keySpaceSize);
    
    TKeyGenerator keyGenerator(*pKeyDistribution, keySpaceSize);
    
    TDelayTracking delayTracking(0);
    TMetrics metrics(0);
    TLogger logger;
    TSimulationClock clock;
    TEventSchedule schedule(clock, logger);
    
    const size_t replicasCount(2);
    const size_t partitionsCount(2);
    const size_t clientsPerPartitionCount(1);
    const double putToGetRatio(0.5);
    
    TKeyHasher hasher;
    TKeyToPartitionMapper keyToPartitionMapper(partitionsCount, hasher);
    
    ProtocolTraits::TNetworkExchange networkExchange(partitionsCount, replicasCount, schedule, metrics, logger);
    
    ProtocolTraits::TSerializer serializer;
    
    ProtocolTraits::TProtocol protocol(replicasCount,
                                       partitionsCount,
                                       clientsPerPartitionCount,
                                       putToGetRatio,
                                       schedule,
                                       keyGenerator,
                                       keyToPartitionMapper,
                                       networkExchange,
                                       serializer,
                                       randomEngine,
                                       logger,
                                       metrics,
                                       delayTracking);
    
    const size_t numOperations = 100;
    const SimRunner::SimulationInfrastructure::SimulationTimeMicroSeconds interoperationDelay = 1000;
    TSimulationHost simulationHost(protocol, numOperations, interoperationDelay, schedule, logger);
    simulationHost.Run();
    
    return 0;
}

#endif

