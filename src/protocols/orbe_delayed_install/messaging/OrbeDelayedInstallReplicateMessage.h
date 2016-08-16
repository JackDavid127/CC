//
//  OrbeDelayedInstallPutReply.h
//  SimRunner
//
//  Created by Scott on 08/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include "Timing.h"
#include "OrbeDelayedInstall.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace Messaging
            {
                template <typename ProtocolTraits>
                class OrbeDelayedInstallReplicateMessageBase
                {
                protected:
                    typedef typename ProtocolTraits::TStorageKey TKey;
                    typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                    typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                    typedef typename ProtocolTraits::TClientId TClientId;
                    typedef typename ProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                    typedef typename ProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                    typedef typename ProtocolTraits::TItemDependencyTimestamp TItemDependencyTimestamp;
                    typedef typename ProtocolTraits::TItemDependencyReplica TItemDependencyReplica;
                    typedef typename ProtocolTraits::TDeserializedValueType TDeserializedValueType;
                    
                    OrbeDelayedInstallReplicateMessageBase()
                    {
                        
                    }
                    
                public:
                    OrbeDelayedInstallReplicateMessageBase(const TKey& key,
                                                           const TClientInputValueType& value,
                                                           const TLogicalTimestamp& logicalTimestamp,
                                                           const TItemDependencyReplica& itemDependencyReplica,
                                                           const TItemDependencyTimestamp& itemDependencyTimestamp,
                                                           const TClientId& clientId,
                                                           const TClientDependencyTimestamp& clientDependencyTimestamp,
                                                           const TClientDependencyPartition& clientDependencyPartition,
                                                           const Utilities::TTimestamp& timestamp,
                                                           ReplicaIdType sourceReplicaId,
                                                           PartitionIdType mutualPartitionId)
                    : m_key(key)
                    , m_value(value)
                    , m_logicalTimestamp(logicalTimestamp)
                    , m_itemDependencyReplica(itemDependencyReplica)
                    , m_itemDependencyTimestamp(itemDependencyTimestamp)
                    , m_clientId(clientId)
                    , m_clientDependencyTimestamp(clientDependencyTimestamp)
                    , m_clientDependencyPartition(clientDependencyPartition)
                    , m_timestamp(timestamp)
                    , m_sourceReplicaId(sourceReplicaId)
                    , m_mutualPartitionId(mutualPartitionId)
                    {
                        
                    }
                    
                    TKey Key() const
                    {
                        return m_key;
                    }
                    
                    TClientInputValueType Value() const
                    {
                        return m_value;
                    }
                    
                    const TClientId& ClientId() const
                    {
                        return m_clientId;
                    }
                    
                    const TClientDependencyTimestamp& ClientDependencyTimestamp() const
                    {
                        return m_clientDependencyTimestamp;
                    }
                    
                    const TClientDependencyPartition& ClientDependencyPartition() const
                    {
                        return m_clientDependencyPartition;
                    }
                    
                    const Utilities::TTimestamp& Timestamp() const
                    {
                        return m_timestamp;
                    }
                    
                    const TItemDependencyReplica& ItemDependencyReplica() const
                    {
                        return m_itemDependencyReplica;
                    }
                    
                    const TItemDependencyTimestamp& ItemDependencyTimestamp() const
                    {
                        return m_itemDependencyTimestamp;
                    }
                    
                    const TLogicalTimestamp& LogicalTimestamp() const
                    {
                        return m_logicalTimestamp;
                    }
                    
                    ReplicaIdType SourceReplicaId() const
                    {
                        return m_sourceReplicaId;
                    }
                    
                    PartitionIdType PartitionServerId() const
                    {
                        return m_mutualPartitionId;
                    }
                    
                    TDeserializedValueType ToValue() const
                    {
                        return TDeserializedValueType(m_key,
                                                      m_value,
                                                      m_logicalTimestamp,
                                                      m_itemDependencyTimestamp,
                                                      m_itemDependencyReplica,
                                                      m_clientId,
                                                      m_clientDependencyTimestamp,
                                                      m_clientDependencyPartition,
                                                      m_timestamp,
                                                      m_sourceReplicaId);
                    }
                    
                    
                private:
                    TKey m_key;
                    TClientInputValueType m_value;
                    TLogicalTimestamp m_logicalTimestamp;
                    TItemDependencyReplica m_itemDependencyReplica;
                    TItemDependencyTimestamp m_itemDependencyTimestamp;
                    TClientId m_clientId;
                    TClientDependencyTimestamp m_clientDependencyTimestamp;
                    TClientDependencyPartition m_clientDependencyPartition;
                    Utilities::TTimestamp m_timestamp;
                    ReplicaIdType m_sourceReplicaId;
                    PartitionIdType m_mutualPartitionId;
                };
                
                template <typename ProtocolTraits>
                class OrbeDelayedInstallReplicateMessage
                : public OrbeDelayedInstallReplicateMessageBase<ProtocolTraits>, private boost::noncopyable
                {
                public:
                    OrbeDelayedInstallReplicateMessage(const typename ProtocolTraits::TStorageKey& key,
                                                       const typename ProtocolTraits::TClientInputValueType& value,
                                                       const typename ProtocolTraits::TLogicalTimestamp& logicalTimestamp,
                                                       const typename ProtocolTraits::TItemDependencyReplica& itemDependencyReplica,
                                                       const typename ProtocolTraits::TItemDependencyTimestamp& itemDependencyTimestamp,
                                                       const typename ProtocolTraits::TClientId& clientId,
                                                       const typename ProtocolTraits::TClientDependencyTimestamp& clientDependencyTimestamp,
                                                       const typename ProtocolTraits::TClientDependencyPartition& clientDependencyPartition,
                                                       const Utilities::TTimestamp& timestamp,
                                                       ReplicaIdType sourceReplicaId,
                                                       PartitionIdType mutualPartitionId)
                    : OrbeDelayedInstallReplicateMessageBase<ProtocolTraits>
                    (key,
                     value,
                     logicalTimestamp,
                     itemDependencyReplica,
                     itemDependencyTimestamp,
                     clientId,
                     clientDependencyTimestamp,
                     clientDependencyPartition,
                     timestamp,
                     sourceReplicaId,
                     mutualPartitionId)
                    { }
                };
                
                template <typename ProtocolTraits>
                class OrbeDelayedInstallReplicateMessageSimulation
                : public OrbeDelayedInstallReplicateMessageBase<ProtocolTraits>
                {
                public:
                    OrbeDelayedInstallReplicateMessageSimulation() { }
                    
                    OrbeDelayedInstallReplicateMessageSimulation(const typename ProtocolTraits::TStorageKey& key,
                                                                 const typename ProtocolTraits::TClientInputValueType& value,
                                                                 const typename ProtocolTraits::TLogicalTimestamp& logicalTimestamp,
                                                                 const typename ProtocolTraits::TItemDependencyReplica& itemDependencyReplica,
                                                                 const typename ProtocolTraits::TItemDependencyTimestamp& itemDependencyTimestamp,
                                                                 const typename ProtocolTraits::TClientId& clientId,
                                                                 const typename ProtocolTraits::TClientDependencyTimestamp& clientDependencyTimestamp,
                                                                 const typename ProtocolTraits::TClientDependencyPartition& clientDependencyPartition,
                                                                 const Utilities::TTimestamp& timestamp,
                                                                 ReplicaIdType sourceReplicaId,
                                                                 PartitionIdType mutualPartitionId)
                    : OrbeDelayedInstallReplicateMessageBase<ProtocolTraits>
                    (key,
                     value,
                     logicalTimestamp,
                     itemDependencyReplica,
                     itemDependencyTimestamp,
                     clientId,
                     clientDependencyTimestamp,
                     clientDependencyPartition,
                     timestamp,
                     sourceReplicaId,
                     mutualPartitionId)
                    { }
                };
            }
        }
    }
}
