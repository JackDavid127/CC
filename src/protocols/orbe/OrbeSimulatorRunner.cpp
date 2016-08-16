//
//  OrbeSimulatorRunner.cpp
//  SimRunner
//
//  Created by Scott on 27/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

//#define RUN_AS_ORBE_SIMULATOR
#ifdef RUN_AS_ORBE_SIMULATOR

#include <iostream>
#include <memory>
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
#include "OrbePartitionServer.h"
#include "OrbeSimulatedExchange.h"
#include "OrbeClient.h"
#include "OrbeSimulation.h"
#include "OrbeSerializer.h"
#include "OrbeGetReply.h"
#include "OrbePutReply.h"
#include "OrbeItem.h"
#include "OrbeReplicateMessage.h"
#include "OrbeItemData.h"
#include "DependencyMatrix.h"
#include "VersionVector.h"
#include "OrbePartitionServerReplicationHandler.h"

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
    
    // Orbe protocol
    using namespace SimRunner::Protocols;
    using namespace SimRunner::Protocols::Orbe;
    using namespace SimRunner::Protocols::Orbe::Messaging;
    using namespace SimRunner::Protocols::Orbe::SimulatedComponents;
    
    // client input value -- string or int
    // partition server item -- OrbeItem containing client input + metadata
    // serialized value -- OrbeItem serialized to string -- TSerializedData
    // client output value -- OrbeGetReply, OrbePutReply
    typedef int TClientInputValueType;
    
    // base simulation
    using namespace SimRunner::SimulationInfrastructure;
    typedef SimulationClock TSimulationClock;
    typedef SimulationEventSchedule<TSimulationClock, TLogger> TEventSchedule;
    
    // components
    using namespace SimRunner::Components;
    
    struct ProtocolTraits
    {
        static const size_t NumPartitions = 2;
        static const size_t NumReplicas = 2;
        
        typedef TEventSchedule TEventSchedule;
        typedef TMetrics TMetrics;
        typedef TMetrics TDelayTracking;
        typedef TLogger TLogger;
        
        typedef OrbeItemData<ProtocolTraits> TSerializedDataType;
        typedef OrbeItem<ProtocolTraits> TDeserializedValueType;
        typedef OrbeGetReplySimulation<ProtocolTraits> TClientGetReplyValueType;
        typedef OrbePutReplySimulation<ProtocolTraits> TClientPutReplyValueType;
        typedef OrbeReplicateMessageSimulation<ProtocolTraits> TReplicationMessageType;
        typedef KeyValueStorage<TStorageKey, TSerializedDataType> TStorage;
        
        typedef TStorageKey TStorageKey;
        typedef int32_t TItemDependencyTimestamp;
        typedef SimRunner::Utilities::VersionVector<NumReplicas, uint32_t> TVersionVector;
        typedef DependencyMatrix<NumPartitions, NumReplicas, uint32_t> TDependencyMatrix;
        
        typedef OrbeSerializer<TSerializedDataType, TDeserializedValueType> TSerializer;
        
        typedef TClientInputValueType TClientInputValueType;
        
        typedef int32_t TLogicalTimestamp;
        
        typedef SimulatedStorageComponent<ProtocolTraits> TStorageComponent;
        
        typedef SimulatedNetworkLinkComponent<TEventSchedule> TNetworkLink;
        
        typedef OrbeSimulatedExchange<TEventSchedule, ProtocolTraits> TNetworkExchange;
        
        typedef OrbePartitionServer<ProtocolTraits> TPartitionServer;
        
        typedef OrbeClient<ProtocolTraits> TClient;
        
        typedef PartitionReplicaBroadcaster<TNetworkExchange, TMetrics> TBroadcaster;
        
        typedef TClient TStorageComponentRequestMetaData;
        
        typedef TKeyToPartitionMapper TKeyToPartitionMapper;
        
        typedef OrbeSimulation<
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
        
        typedef std::shared_ptr<TProtocol> TProtocolPtr;
        
        typedef TProtocolPtr TClientQueryResponseHandlerPtr;
        
        typedef OrbePartitionServerReplicationHandler<ProtocolTraits> TReplicationHandler;
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
    
    const size_t replicasCount(ProtocolTraits::NumReplicas);
    const size_t partitionsCount(ProtocolTraits::NumPartitions);
    const size_t clientsPerPartitionCount(1);
    const double putToGetRatio(0.5);
    
    TKeyHasher hasher;
    TKeyToPartitionMapper keyToPartitionMapper(partitionsCount, hasher);
    
    ProtocolTraits::TNetworkExchange networkExchange(partitionsCount, replicasCount, schedule, metrics, logger);
    
    ProtocolTraits::TSerializer serializer;
    
    ProtocolTraits::TProtocolPtr pProtocol = ProtocolTraits::TProtocol::Create(replicasCount,
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
    TSimulationHost simulationHost(*pProtocol, numOperations, interoperationDelay, schedule, logger);
    simulationHost.Run();
    
    return 0;
}

#endif

