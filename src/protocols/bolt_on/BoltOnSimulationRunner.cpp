//
//  BoltOnSimulationRunner.cpp
//  SimRunner
//
//  Created by Scott on 15/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#define RUN_AS_BOLT_ON_SIMULATOR
#ifdef RUN_AS_BOLT_ON_SIMULATOR

#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <iostream>
#include <memory>
#include "SimRunnerAssert.h"
#include "StandardErrorLogger.h"
#include "SimulationClock.h"
#include "SimulationEventSchedule.h"
#include "SimulationHost.h"
#include "UniformDistribution.h"
#include "KeyGenerator.h"
#include "SimulatedNetworkLinkComponent.h"
#include "KeyToPartitionMapper.h"
#include "StringHashProvider.h"
#include "PartitionReplicaBroadcaster.h"
#include "AsynchronousReadShimBackEnd.h"
#include "AsynchronousResolver.h"
#include "BoltOn.h"
#include "BoltOnClient.h"
#include "BoltOnSerializer.h"
#include "BoltOnSimulatedBackingStorage.h"
#include "BoltOnSimulation.h"
#include "BoltOnVersionApplier.h"
#include "CausalClock.h"
#include "CausalRelationship.h"
#include "KeyDependencies.h"
#include "ValueWrapperData.h"
#include "ShimDeployment.h"
#include "ValueWrapper.h"
#include "ValueWrapperFactory.h"

typedef int TUnknown;

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

template <typename TProtocolTraits>
class ScheduleBasedTaskPool
{
    typedef typename TProtocolTraits::TEventSchedule TEventSchedule;
    
    TEventSchedule& m_schedule;
    
public:
    ScheduleBasedTaskPool(TEventSchedule& schedule)
    : m_schedule(schedule)
    {
        
    }
    
    void Schedule(boost::function<void ()>& task)
    {
        auto wrapper = m_schedule.BuildSimulationEventTimeStampedNow(task, "ScheduleBasedTaskPool event.");
        m_schedule.InsertSimulationEvent(wrapper, 0);
    }
};

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
    
    // BoltOn protocol
    using namespace SimRunner::Protocols;
    using namespace SimRunner::Protocols::BoltOn;
    using namespace SimRunner::Protocols::BoltOn::SimulatedComponents;
    
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
        typedef TRandomEngine TRandomEngine;
        
        typedef TUnknown TReplicationMessageType;
        
        typedef BoltOnSimulatedBackingStorage<ProtocolTraits> TBackingStorage;
        
        typedef size_t TShimId;
        typedef size_t TCounterValue;
        typedef SimpleCounter<ProtocolTraits> TCounter;
        typedef std::map<TShimId, TCounter> TShimIdToWritesMapping;
        
        typedef ValueWrapper<ProtocolTraits> TValueWrapper;
        typedef ValueWrapperFactory<ProtocolTraits> TValueWrapperFactory;
        typedef std::shared_ptr<TValueWrapper> TValueWrapperPtr;
        typedef KeyValueStorage<TStorageKey, TValueWrapperPtr> TLocalShimStorage;
        
        typedef TStorageKey TStorageKey;
        typedef CausalClock<ProtocolTraits> TCausalClock;
        typedef AsynchronousResolver<ProtocolTraits> TLocalStoreResolver;
        typedef KeyDependencies<ProtocolTraits> TKeyDependencies;
        typedef AsynchronousReadShimBackEnd<ProtocolTraits> TShimBackEnd;
        typedef int32_t TClientInputValueType;
        typedef std::map<TStorageKey, TCausalClock> TKeyDependenciesMapping;
        typedef ShimDeployment<ProtocolTraits> TShimDeployment;
        typedef ScheduleBasedTaskPool<ProtocolTraits> TTaskRunner;
        typedef BoltOnVersionApplier<ProtocolTraits> TVersionApplier;
        
        typedef TKeyGenerator TKeyGenerator;
        typedef int32_t TItemDependencyTimestamp;
        typedef int32_t TLogicalTimestamp;
        
        typedef BoltOnSerializer<ProtocolTraits> TBoltOnSerializer;
        typedef ValueWrapperData<ProtocolTraits> TValueWrapperData;
        typedef ValueWrapperDataFactory<ProtocolTraits> TValueWrapperDataFactory;
        typedef std::shared_ptr<TValueWrapperData> TBoltOnSerializedValueTypePtr;
        
        typedef KeyValueStorage<TStorageKey, TBoltOnSerializedValueTypePtr> TStorage;
        
        typedef BoltOnClient<ProtocolTraits> TBoltOnClient;
        typedef TBoltOnClient TBoltOnStorageComponentRequestMetaData;
        
        typedef BoltOnSimulation<ProtocolTraits> TProtocol;
        
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
    
    const size_t replicasCount(ProtocolTraits::NumReplicas);
    const size_t partitionsCount(ProtocolTraits::NumPartitions);
    const size_t clientsPerPartitionCount(1);
    const double putToGetRatio(0.5);
    
    TKeyHasher hasher;
    TKeyToPartitionMapper keyToPartitionMapper(partitionsCount, hasher);
    
    ProtocolTraits::TValueWrapperDataFactory valueWrapperDataFactory;
    ProtocolTraits::TValueWrapperFactory valueWrapperFactory(valueWrapperDataFactory);
    ProtocolTraits::TBoltOnSerializer serializer(valueWrapperFactory);
    ProtocolTraits::TTaskRunner taskRunner(schedule);
    
    ProtocolTraits::TProtocol protocol(replicasCount,
                                       clientsPerPartitionCount,
                                       keyGenerator,
                                       schedule,
                                       randomEngine,
                                       metrics,
                                       logger,
                                       serializer,
                                       taskRunner,
                                       valueWrapperFactory,
                                       putToGetRatio);
    
    const size_t numOperations = 100;
    const SimRunner::SimulationInfrastructure::SimulationTimeMicroSeconds interoperationDelay = 1000;
    TSimulationHost simulationHost(protocol, numOperations, interoperationDelay, schedule, logger);
    simulationHost.Run();
    
    return 0;
}

#endif

