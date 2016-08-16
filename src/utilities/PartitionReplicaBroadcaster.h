//
//  PartitionReplicaBroadcaster.h
//  SimRunner
//
//  Created by Scott on 25/09/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include <set>
#include "SimRunnerAssert.h"
#include "Protocols.h"

namespace SimRunner
{
    namespace Utilities
    {
        template <typename TNetworkExchange, typename TMetrics>
        class PartitionReplicaBroadcaster : private boost::noncopyable
        {
        public:
            PartitionReplicaBroadcaster(const Protocols::ReplicaIdType& replicaId,
                                        const Protocols::PartitionIdType& partitionId,
                                        TNetworkExchange& networkExchange,
                                        TMetrics& metrics)
            : m_replicaId(replicaId)
            , m_partitionId(partitionId)
            , m_networkExchange(networkExchange)
            , m_metrics(metrics)
            {
                
            }
            
            void InsertLinkToRemoteReplica(const Protocols::ReplicaIdType& replicaId)
            {
                SR_ASSERT(replicaId != m_replicaId);
                m_knownReplicas.insert(replicaId);
            }
            
            void InsertLinkToLocalPartitionNeighbour(const Protocols::PartitionIdType& partitionId)
            {
                SR_ASSERT(partitionId != m_partitionId);
                m_knownLocalPartitionNeighbours.insert(partitionId);
            }
            
            template <typename TReplicationMessage>
            void BroadcastReplicationToPartitionReplicas(const TReplicationMessage& replicationMessage)
            {
                const Protocols::ReplicaIdType& sourceReplicaId = replicationMessage.SourceReplicaId();
                SR_ASSERT(sourceReplicaId == m_replicaId);
                
                for(const auto& receiverReplicaId : m_knownReplicas)
                {
                    //self.__metrics.add_replicate_message_sent_from_data_centre_replica(receiver_replica_id, source_replica_id)
                    
                    m_networkExchange.SendReplicationMessageToReplica(m_replicaId,
                                                                      receiverReplicaId,
                                                                      m_partitionId,
                                                                      replicationMessage);
                }
            }
            
            template <typename TVersionVector>
            void BroadcastHeartbeatVectorToPartitionReplicas(const Protocols::ReplicaIdType& sourceReplicaId, const TVersionVector& versionVector)
            {
                SR_ASSERT(sourceReplicaId == m_replicaId);
                
                for(const auto& receiverReplicaId : m_knownReplicas)
                {
                    //self.__metrics.add_heartbeat_message_sent_from_data_centre_replica(receiver_replica_id, source_replica_id)
                    
                    m_networkExchange.SendHeartbeatVectorMessageToReplica(m_replicaId,
                                                                          receiverReplicaId,
                                                                          m_partitionId,
                                                                          versionVector);
                }
            }
            
        private:
            const Protocols::ReplicaIdType m_replicaId;
            const Protocols::PartitionIdType m_partitionId;
            TNetworkExchange& m_networkExchange;
            TMetrics& m_metrics;
            std::set<Protocols::ReplicaIdType> m_knownReplicas;
            std::set<Protocols::PartitionIdType> m_knownLocalPartitionNeighbours;
            
        };
    }
}