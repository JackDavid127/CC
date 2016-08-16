//
//  OrbePutReply.h
//  SimRunner
//
//  Created by Scott on 08/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include "Timing.h"
#include "Orbe.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace Messaging
            {
                template <typename ProtocolTraits>
                class OrbeReplicateMessageBase
                {
                protected:
                    typedef typename ProtocolTraits::TStorageKey TKey;
                    typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                    typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                    typedef typename ProtocolTraits::TDependencyMatrix TDependencyMatrix;
                    typedef typename ProtocolTraits::TItemDependencyTimestamp TItemDependencyTimestamp;
                    typedef typename ProtocolTraits::TDeserializedValueType TDeserializedValueType;
                    
                    OrbeReplicateMessageBase()
                    {
                        
                    }
                    
                public:
                    OrbeReplicateMessageBase(const TKey& key,
                                             const TClientInputValueType& value,
                                             const TLogicalTimestamp& logicalTimestamp,
                                             const TItemDependencyTimestamp& itemDependencyTimestamp,
                                             const TDependencyMatrix& dependencyMatrix,
                                             const Utilities::TTimestamp& timestamp,
                                             ReplicaIdType sourceReplicaId,
                                             PartitionIdType mutualPartitionId)
                    : m_key(key)
                    , m_value(value)
                    , m_logicalTimestamp(logicalTimestamp)
                    , m_itemDependencyTimestamp(itemDependencyTimestamp)
                    , m_dependencyMatrix(dependencyMatrix)
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
                    
                    const TDependencyMatrix& Matrix() const
                    {
                        return m_dependencyMatrix;
                    }
                    
                    const Utilities::TTimestamp& Timestamp() const
                    {
                        return m_timestamp;
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
                                                      m_dependencyMatrix,
                                                      m_timestamp,
                                                      m_sourceReplicaId);
                    }
                    
                    
                private:
                    TKey m_key;
                    TClientInputValueType m_value;
                    TLogicalTimestamp m_logicalTimestamp;
                    TItemDependencyTimestamp m_itemDependencyTimestamp;
                    TDependencyMatrix m_dependencyMatrix;
                    Utilities::TTimestamp m_timestamp;
                    ReplicaIdType m_sourceReplicaId;
                    PartitionIdType m_mutualPartitionId;
                };
                
                template <typename ProtocolTraits>
                class OrbeReplicateMessage
                : public OrbeReplicateMessageBase<ProtocolTraits>, private boost::noncopyable
                {
                public:
                    OrbeReplicateMessage(const typename ProtocolTraits::TStorageKey& key,
                                         const typename ProtocolTraits::TClientInputValueType& value,
                                         const typename ProtocolTraits::TLogicalTimestamp& logicalTimestamp,
                                         const typename ProtocolTraits::TItemDependencyTimestamp& itemDependencyTimestamp,
                                         const typename ProtocolTraits::TDependencyMatrix& dependencyMatrix,
                                         const Utilities::TTimestamp& timestamp,
                                         ReplicaIdType sourceReplicaId,
                                         PartitionIdType mutualPartitionId)
                    : OrbeReplicateMessageBase<ProtocolTraits>
                    (key, value, logicalTimestamp, itemDependencyTimestamp, dependencyMatrix, timestamp, sourceReplicaId, mutualPartitionId)
                    { }
                };
                
                template <typename ProtocolTraits>
                class OrbeReplicateMessageSimulation
                : public OrbeReplicateMessageBase<ProtocolTraits>
                {
                public:
                    OrbeReplicateMessageSimulation() { }
                    
                    OrbeReplicateMessageSimulation(const typename ProtocolTraits::TStorageKey& key,
                                                   const typename ProtocolTraits::TClientInputValueType& value,
                                                   const typename ProtocolTraits::TLogicalTimestamp& logicalTimestamp,
                                                   const typename ProtocolTraits::TItemDependencyTimestamp& itemDependencyTimestamp,
                                                   const typename ProtocolTraits::TDependencyMatrix& dependencyMatrix,
                                                   const Utilities::TTimestamp& timestamp,
                                                   ReplicaIdType sourceReplicaId,
                                                   PartitionIdType mutualPartitionId)
                    : OrbeReplicateMessageBase<ProtocolTraits>
                    (key, value, logicalTimestamp, itemDependencyTimestamp, dependencyMatrix, timestamp, sourceReplicaId, mutualPartitionId)
                    { }
                };
            }
        }
    }
}
