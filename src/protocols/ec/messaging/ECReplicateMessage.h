//
//  ECPutReply.h
//  SimRunner
//
//  Created by Scott on 08/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include "Timing.h"
#include "EC.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            namespace Messaging
            {
                template <
                typename TKey,
                typename TClientInputValueType,
                typename TDeserializedValueType,
                typename TImplementation>
                class ECReplicateMessageBase
                {
                protected:
                    ECReplicateMessageBase()
                    {
                        
                    }
                    
                public:
                    ECReplicateMessageBase(const TKey& key,
                                           const TClientInputValueType& value,
                                           const Utilities::TTimestamp& timestamp,
                                           ReplicaIdType sourceReplicaId,
                                           PartitionIdType mutualPartitionId)
                    : m_key(key)
                    , m_value(value)
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
                    
                    const Utilities::TTimestamp& Timestamp() const
                    {
                        return m_timestamp;
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
                                                      m_timestamp,
                                                      m_sourceReplicaId);
                    }
                    
                    
                private:
                    TKey m_key;
                    TClientInputValueType m_value;
                    Utilities::TTimestamp m_timestamp;
                    ReplicaIdType m_sourceReplicaId;
                    PartitionIdType m_mutualPartitionId;
                };

                template <
                typename TKey,
                typename TClientInputValueType,
                typename TDeserializedValueType>
                class ECReplicateMessage
                : public ECReplicateMessageBase<TKey, TClientInputValueType, TDeserializedValueType, ECReplicateMessage<TKey, TClientInputValueType, TDeserializedValueType> >, private boost::noncopyable 
                {
                public:
                    ECReplicateMessage(const TKey& key,
                                       const TClientInputValueType& value,
                                       const Utilities::TTimestamp& timestamp,
                                       ReplicaIdType sourceReplicaId,
                                       PartitionIdType mutualPartitionId)
                    : ECReplicateMessageBase<TKey, TClientInputValueType, TDeserializedValueType, ECReplicateMessage<TKey, TClientInputValueType, TDeserializedValueType> >
                    (key, value, timestamp, sourceReplicaId, mutualPartitionId)
                    { }
                };
                
                template <
                typename TKey,
                typename TClientInputValueType,
                typename TDeserializedValueType>
                class ECReplicateMessageSimulation
                : public ECReplicateMessageBase<TKey, TClientInputValueType, TDeserializedValueType, ECReplicateMessageSimulation <TKey, TClientInputValueType, TDeserializedValueType> >
                {
                public:
                    ECReplicateMessageSimulation() { }
                    
                    ECReplicateMessageSimulation(const TKey& key,
                                                 const TClientInputValueType& value,
                                                 const Utilities::TTimestamp& timestamp,
                                                 ReplicaIdType sourceReplicaId,
                                                 PartitionIdType mutualPartitionId)
                    : ECReplicateMessageBase<TKey, TClientInputValueType, TDeserializedValueType, ECReplicateMessageSimulation <TKey, TClientInputValueType, TDeserializedValueType> >
                    (key, value, timestamp, sourceReplicaId, mutualPartitionId)
                    { }
                };
            }
        }
    }
}
