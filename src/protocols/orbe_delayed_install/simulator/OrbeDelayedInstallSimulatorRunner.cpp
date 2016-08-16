//
//  OrbeDelayedInstallSimulatorRunner.cpp
//  SimRunner
//
//  Created by Scott on 27/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

//#define RUN_AS_ORBE_DELAYED_INSTALL_SIMULATOR
#ifdef RUN_AS_ORBE_DELAYED_INSTALL_SIMULATOR

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
#include "OrbeDelayedInstallPartitionServer.h"
#include "OrbeDelayedInstallSimulatedExchange.h"
#include "OrbeDelayedInstallClient.h"
#include "OrbeDelayedInstallSimulation.h"
#include "OrbeDelayedInstallSerializer.h"
#include "OrbeDelayedInstallGetReply.h"
#include "OrbeDelayedInstallPutReply.h"
#include "OrbeDelayedInstallItem.h"
#include "OrbeDelayedInstallReplicateMessage.h"
#include "OrbeDelayedInstallItemData.h"
#include "VersionVector.h"
#include "OrbeDelayedInstallPartitionServerReplicationHandler.h"
#include "OrbeDelayedInstallClientUpdateSpace.h"
#include "OrbeDelayedInstallReplicaUpdateSpace.h"

class NullLogger
{
public:
    void Log(const char* format, ...) { }
};

namespace
{
    typedef std::mt19937 TRandomEngine;
    
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
    typedef Distributions::UniformDistribution<TRandomEngine> TKeyDistribution;
    typedef KeyGenerator<TKeyDistribution> TKeyGenerator;
    typedef KeyToPartitionMapper<TStorageKey, TKeyHasher> TKeyToPartitionMapper;
    
    // OrbeDelayedInstall protocol
    using namespace SimRunner::Protocols;
    using namespace SimRunner::Protocols::OrbeDelayedInstall;
    using namespace SimRunner::Protocols::OrbeDelayedInstall::Messaging;
    using namespace SimRunner::Protocols::OrbeDelayedInstall::SimulatedComponents;
    
    // client input value -- string or int
    // partition server item -- OrbeDelayedInstallItem containing client input + metadata
    // serialized value -- OrbeDelayedInstallItem serialized to string -- TSerializedData
    // client output value -- OrbeDelayedInstallGetReply, OrbeDelayedInstallPutReply
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
        
        typedef OrbeDelayedInstallItemData<ProtocolTraits> TSerializedDataType;
        typedef OrbeDelayedInstallItem<ProtocolTraits> TDeserializedValueType;
        typedef OrbeDelayedInstallGetReplySimulation<ProtocolTraits> TClientGetReplyValueType;
        typedef OrbeDelayedInstallPutReplySimulation<ProtocolTraits> TClientPutReplyValueType;
        typedef OrbeDelayedInstallReplicateMessageSimulation<ProtocolTraits> TReplicationMessageType;
        typedef KeyValueStorage<TStorageKey, TSerializedDataType> TStorage;
        
        typedef TStorageKey TStorageKey;
        typedef int32_t TItemDependencyTimestamp;
        typedef SimRunner::Utilities::VersionVector<NumReplicas, uint32_t> TVersionVector;
        
        typedef ReplicaIdType TItemDependencyReplica;
        typedef ClientIdType TClientId;
        typedef int32_t TClientDependencyTimestamp;
        typedef PartitionIdType TClientDependencyPartition;
        
        typedef OrbeDelayedInstallSerializer<TSerializedDataType, TDeserializedValueType> TSerializer;
        
        typedef TClientInputValueType TClientInputValueType;
        
        typedef int32_t TLogicalTimestamp;
        
        typedef OrbeDelayedInstallClientUpdateSpace<ProtocolTraits> TClientUpdateSpace;
        typedef OrbeDelayedInstallReplicaUpdateSpace<ProtocolTraits> TReplicaUpdateSpace;
        
        typedef SimulatedStorageComponent<ProtocolTraits> TStorageComponent;
        
        typedef SimulatedNetworkLinkComponent<TEventSchedule> TNetworkLink;
        
        typedef OrbeDelayedInstallSimulatedExchange<TEventSchedule, ProtocolTraits> TNetworkExchange;
        
        typedef OrbeDelayedInstallPartitionServer<ProtocolTraits> TPartitionServer;
        
        typedef OrbeDelayedInstallClient<ProtocolTraits> TClient;
        
        typedef PartitionReplicaBroadcaster<TNetworkExchange, TMetrics> TBroadcaster;
        
        typedef TClient TStorageComponentRequestMetaData;
        
        typedef TKeyToPartitionMapper TKeyToPartitionMapper;
        
        typedef OrbeDelayedInstallSimulation<
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
        
        typedef OrbeDelayedInstallPartitionServerReplicationHandler<ProtocolTraits> TReplicationHandler;
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
    
    const size_t numOperations = 1000;
    const SimRunner::SimulationInfrastructure::SimulationTimeMicroSeconds interoperationDelay = 1000;
    TSimulationHost simulationHost(*pProtocol, numOperations, interoperationDelay, schedule, logger);
    simulationHost.Run();
    
    return 0;
}

#endif

