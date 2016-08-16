//
//  OrbeInFlightReplication.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include <deque>
#include "SimRunnerAssert.h"
#include "Timing.h"
#include "Orbe.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            template<typename TProtocolTraits>
            class OrbeInFlightReplication
            {
                typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                typedef typename TProtocolTraits::TDependencyMatrix TDependencyMatrix;
                typedef typename TProtocolTraits::TVersionVector TVersionVector;
                typedef typename TProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                typedef typename TProtocolTraits::TReplicationMessageType TReplicationMessageType;
                typedef typename TProtocolTraits::TLogger TLogger;
                typedef typename TProtocolTraits::TReplicationHandler TReplicationHandler;
                
            public:
                OrbeInFlightReplication(TLogger& logger,
                                        TReplicationHandler& replicationHandler,
                                        const TPartitionServer& partitionServer,
                                        const TReplicationMessageType& replicateMessage)
                : m_logger(logger)
                , m_replicationHandler(replicationHandler)
                , m_partitionServer(partitionServer)
                , m_replicateMessage(replicateMessage)
                , m_partitionsCausalityVerified(0)
                , m_requiredPartitionCausalityVerifications(0)
                , m_isActive(false)
                , m_retrievedReplication(false)
                , m_hasInstalledDependencies(false)
                , m_dependencyChecksDispatched(false)
                {
                    const TDependencyMatrix& matrix(m_replicateMessage.Matrix());
                    
                    for(size_t i = 0; i < matrix.NumPartitions(); ++ i)
                    {
                        const PartitionIdType partitionId(matrix.GetPartitionId(i));
                        
                        if(partitionId != m_partitionServer.PartitionId() && !matrix.IsEmpty(partitionId))
                        {
                            m_requiredPartitionCausalityVerifications += 1;
                        }
                    }
                }
                
                const TReplicationMessageType& ReplicateMessage()
                {
                    SR_ASSERT(IsActive());
                    SR_ASSERT(!m_retrievedReplication);
                    m_retrievedReplication = true;
                    return m_replicateMessage;
                }
                
                bool IsActive() const { return m_isActive; }
                
                TLogicalTimestamp LogicalTimestamp() const
                {
                    SR_ASSERT(IsActive());
                    return m_replicateMessage.LogicalTimestamp();
                }
                
                void Activate()
                {
                    SR_ASSERT(!IsActive());
                    m_isActive = true;
                }
                
                bool BlockedOnNeighbouringPartitions() const
                {
                    return (m_partitionsCausalityVerified < m_requiredPartitionCausalityVerifications);
                }
                
                bool DependenciesAreInstalled(const TVersionVector& logicalVersionVector,
                                              const PartitionIdType& partitionId)
                {
                    if(!m_hasInstalledDependencies)
                    {
                        TVersionVector partitionVector;
                        m_replicateMessage.Matrix().ComputeVersionVector(partitionId, partitionVector);
                        m_hasInstalledDependencies = logicalVersionVector.GreaterOrEquals(partitionVector);
                        SR_ASSERT(m_hasInstalledDependencies);
                    }
                    
                    return m_hasInstalledDependencies;
                }
                
                void DispatchNeighbourPartitionDependencyChecks()
                {
                    SR_ASSERT(!m_dependencyChecksDispatched);
                    m_dependencyChecksDispatched = true;
                    
                    const TDependencyMatrix& matrix(m_replicateMessage.Matrix());
                    
                    for(size_t i = 0; i < matrix.NumPartitions(); ++ i)
                    {
                        const PartitionIdType partitionId(matrix.GetPartitionId(i));
                        
                        if(partitionId != m_partitionServer.PartitionId())
                        {
                            if(!matrix.IsEmpty(partitionId))
                            {
                                TVersionVector vectorToVerify;
                                matrix.ComputeVersionVector(partitionId, vectorToVerify);
                                
                                m_replicationHandler.VerifyNeighbourPartitionVersionVectorGreaterOrEqualTo(m_replicateMessage.SourceReplicaId(),
                                                                                                           partitionId,
                                                                                                           vectorToVerify);
                            }
                        }
                    }
                }
                
                void VerifyNeighbouringPartitionCausality()
                {
                    SR_ASSERT(m_hasInstalledDependencies);
                    SR_ASSERT(BlockedOnNeighbouringPartitions());
                    m_partitionsCausalityVerified += 1;
                }
                
            private:
                TLogger& m_logger;
                TReplicationHandler& m_replicationHandler;
                const TPartitionServer& m_partitionServer;
                TReplicationMessageType m_replicateMessage;
                size_t m_partitionsCausalityVerified;
                size_t m_requiredPartitionCausalityVerifications;
                bool m_isActive;
                bool m_retrievedReplication;
                bool m_hasInstalledDependencies;
                bool m_dependencyChecksDispatched;
            };
        }
    }
}
