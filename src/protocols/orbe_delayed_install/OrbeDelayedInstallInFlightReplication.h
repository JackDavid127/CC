//
//  OrbeDelayedInstallInFlightReplication.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include "SimRunnerAssert.h"
#include "Timing.h"
#include "OrbeDelayedInstall.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template<typename TProtocolTraits>
            class OrbeDelayedInstallInFlightReplication
            {
                typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                typedef typename TProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                typedef typename TProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                typedef typename TProtocolTraits::TDeserializedValueType TDeserializedValueType;
                typedef typename TProtocolTraits::TLogger TLogger;
                typedef typename TProtocolTraits::TReplicationHandler TReplicationHandler;
                
            public:
                OrbeDelayedInstallInFlightReplication(TLogger& logger,
                                                      TReplicationHandler& replicationHandler,
                                                      const TPartitionServer& partitionServer,
                                                      const TDeserializedValueType& value)
                : m_logger(logger)
                , m_replicationHandler(replicationHandler)
                , m_partitionServer(partitionServer)
                , m_value(value)
                , m_isActive(false)
                , m_avoidsNeighbourCheck(false)
                , m_neighbourPartitionCausalityVerified(false)
                , m_blockedOnLocalUpdate(true)
                , m_retrievedReplication(false)
                , m_hasInstalledDependencies(false)
                , m_dependencyChecksDispatched(false)
                {
                    const auto clientDependencyPartition(m_value.ClientDependencyPartition());
                    m_avoidsNeighbourCheck = (clientDependencyPartition == partitionServer.PartitionId() || clientDependencyPartition == -1);
                    m_neighbourPartitionCausalityVerified = m_avoidsNeighbourCheck;
                }
                
                const TDeserializedValueType& Value()
                {
                    SR_ASSERT(IsActive());
                    SR_ASSERT(!m_retrievedReplication);
                    
                    m_retrievedReplication = true;
                    return m_value;
                }
                
                bool IsActive() const { return m_isActive; }
                
                TLogicalTimestamp LogicalTimestamp() const
                {
                    SR_ASSERT(IsActive());
                    
                    return m_value.LogicalTimestamp();
                }
                
                TClientDependencyTimestamp ClientDependencyTimestamp() const
                {
                    SR_ASSERT(IsActive());
                    
                    return m_value.ClientDependencyTimestamp();
                }
                
                void Activate()
                {
                    SR_ASSERT(!IsActive());
                    
                    m_isActive = true;
                }
                
                bool BlockedOnNeighbouringPartitions() const
                {
                    SR_ASSERT(IsActive());
                    
                    return !m_neighbourPartitionCausalityVerified;
                }
                
                bool HasInstalledDependencies() const
                {
                    SR_ASSERT(IsActive());
                    
                    return m_hasInstalledDependencies;
                }
                
                bool DependenciesAreInstalled(const TVersionVector& logicalVersionVector)
                {
                    SR_ASSERT(IsActive());
                    
                    if(!m_hasInstalledDependencies)
                    {
                        const auto currentReplicaValue(logicalVersionVector.GetValueForReplica(m_value.SourceReplicaId()));
                        const auto expectedReplicaValue(m_value.LogicalTimestamp() - 1);
                        m_hasInstalledDependencies = currentReplicaValue == expectedReplicaValue;
                    }
                    
                    return m_hasInstalledDependencies;
                }
                
                void DispatchNeighbourPartitionDependencyChecks()
                {
                    SR_ASSERT(IsActive());
                    
                    if(!m_avoidsNeighbourCheck)
                    {
                        SR_ASSERT(!m_dependencyChecksDispatched);
                        m_dependencyChecksDispatched = true;
                        
                        SR_ASSERT(m_partitionServer.PartitionId() != m_value.ClientDependencyPartition());
                        SR_ASSERT(-1 != m_value.ClientDependencyPartition());
                        
                        m_replicationHandler.VerifyNeighbourPartitionTimestampGreaterOrEqualTo(m_value.ClientDependencyPartition(),
                                                                                               m_value.SourceReplicaId(),
                                                                                               m_value.ClientDependencyTimestamp());
                    }
                }
                
                void VerifyNeighbouringPartitionCausality()
                {
                    SR_ASSERT(IsActive());
                    SR_ASSERT(m_hasInstalledDependencies);
                    SR_ASSERT(BlockedOnNeighbouringPartitions());
                    
                    m_neighbourPartitionCausalityVerified = true;
                }
                
            protected:
                bool AvoidsDependencyCheck() const
                {
                    SR_ASSERT(IsActive());
                    
                    return m_avoidsNeighbourCheck;
                }
                
                bool DependencyChecksDispatched() const
                {
                    SR_ASSERT(IsActive());
                    
                    return m_dependencyChecksDispatched;
                }
                
                void EnforceDependencyCheckDispatched()
                {
                    SR_ASSERT(IsActive());
                    
                    m_dependencyChecksDispatched = true;
                }
                
            private:
                TLogger& m_logger;
                TReplicationHandler& m_replicationHandler;
                const TPartitionServer& m_partitionServer;
                TDeserializedValueType m_value;
                
                bool m_isActive;
                bool m_avoidsNeighbourCheck;
                bool m_neighbourPartitionCausalityVerified;
                bool m_blockedOnLocalUpdate;
                bool m_hasInstalledDependencies;
                bool m_retrievedReplication;
                bool m_dependencyChecksDispatched;
            };
            
            template<typename TProtocolTraits>
            class OrbeDelayedInstallInFlightApplicationToGlobalSpace : public OrbeDelayedInstallInFlightReplication<TProtocolTraits>
            {
                typedef OrbeDelayedInstallInFlightApplicationToGlobalSpace<TProtocolTraits> TBase;
                
                typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                typedef typename TProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                typedef typename TProtocolTraits::TDeserializedValueType TDeserializedValueType;
                typedef typename TProtocolTraits::TLogger TLogger;
                typedef typename TProtocolTraits::TReplicationHandler TReplicationHandler;
                
            public:
                OrbeDelayedInstallInFlightApplicationToGlobalSpace(TLogger& logger,
                                                                   TReplicationHandler& replicationHandler,
                                                                   const TPartitionServer& partitionServer,
                                                                   const TDeserializedValueType& value)
                : OrbeDelayedInstallInFlightReplication<TProtocolTraits>(logger, replicationHandler, partitionServer, value)
                , m_replicationHandler(replicationHandler)
                , m_partitionServer(partitionServer)
                , m_value(value)
                , m_blockedOnLocalDependency(true)
                {
                    
                }
                
                void DispatchGlobalNeighbourPartitionDependencyChecks()
                {
                    SR_ASSERT(TBase::IsActive());
                    
                    if(!TBase::AvoidsDependencyCheck())
                    {
                        SR_ASSERT(!TBase::DependencyChecksDispatched());
                        TBase::EnforceDependencyCheckDispatched();
                        
                        SR_ASSERT(m_partitionServer.PartitionId() != m_value.ClientDependencyPartition());
                        SR_ASSERT(-1 != m_value.ClientDependencyPartition());
                        
                        m_replicationHandler.VerifyNeighbourPartitionGlobalTimestampGreaterOrEqualTo(m_value.ClientDependencyPartition(),
                                                                                                     m_value.SourceReplicaId(),
                                                                                                     m_value.ClientDependencyTimestamp());
                    }
                }
                
                bool BlockedOnLocalDependency() const
                {
                    SR_ASSERT(TBase::IsActive());
                    
                    return m_blockedOnLocalDependency;
                }
                
                void Unblock()
                {
                    SR_ASSERT(TBase::IsActive());
                    
                    m_blockedOnLocalDependency = false;
                }
                
            private:
                TReplicationHandler& m_replicationHandler;
                const TPartitionServer& m_partitionServer;
                TDeserializedValueType m_value;
                bool m_blockedOnLocalDependency;
            };
        }
    }
}
