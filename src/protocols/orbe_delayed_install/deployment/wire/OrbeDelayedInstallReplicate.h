//
//  OrbeDelayedInstallReplicate.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "OrbeDelayedInstall.h"
#include "OrbeDelayedInstallWire.h"
#include "Wire.h"
#include "Timing.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace Deployment
            {
                namespace Wire
                {
                    template <typename ProtocolTraits>
                    class OrbeDelayedInstallReplicate
                    {
                        typedef OrbeDelayedInstallReplicate TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        typedef Utilities::TTimestamp TTimestamp;
                        typedef SimRunner::Protocols::ReplicaIdType ReplicaIdType;
                        typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                        typedef typename ProtocolTraits::TItemDependencyTimestamp TItemDependencyTimestamp;
                        typedef typename ProtocolTraits::TItemDependencyReplica TItemDependencyReplica;
                        typedef typename ProtocolTraits::TClientId TClientId;
                        typedef typename ProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                        typedef typename ProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                        
                    public:
                        static TSelf Create(TKeyType key,
                                            TClientInputValueType value,
                                            TTimestamp timestamp,
                                            ReplicaIdType sourceReplicaId,
                                            const TLogicalTimestamp& logicalTimestamp,
                                            const TItemDependencyTimestamp& itemDependencyTimestamp,
                                            const TItemDependencyReplica& itemDependencyReplica,
                                            const TClientId& clientId,
                                            const TClientDependencyTimestamp& clientDependencyTimestamp,
                                            const TClientDependencyPartition& clientDependencyPartition)
                        {
                            return TSelf(key,
                                         value,
                                         timestamp,
                                         sourceReplicaId,
                                         logicalTimestamp,
                                         itemDependencyTimestamp,
                                         itemDependencyReplica,
                                         clientId,
                                         clientDependencyTimestamp,
                                         clientDependencyPartition);
                        }
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        TTimestamp Timestamp() const { return m_timestamp; }
                        
                        ReplicaIdType SourceReplicaId() const { return m_sourceReplicaId; }
                        
                        TLogicalTimestamp LogicalTimestamp() const { return m_logicalTimestamp; }
                        
                        TItemDependencyTimestamp ItemDependencyTimestamp() const { return m_itemDependencyTimestamp; }
                        
                        TItemDependencyReplica ItemDependencyReplica() const { return m_itemDependencyReplica; }
                        
                        TClientId ClientId() const { return m_clientId; }
                        
                        TClientDependencyTimestamp ClientDependencyTimestamp() const { return m_clientDependencyTimestamp; }
                        
                        TClientDependencyPartition ClientDependencyPartition() const { return m_clientDependencyPartition; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            TBufferReader reader(buffer);
                            
                            TKeyType key = reader.Read<TKeyType>();
                            TClientInputValueType value = reader.Read<TClientInputValueType>();
                            TTimestamp timestamp(reader.Read<uint64_t>());
                            ReplicaIdType sourceReplicaId = reader.Read<ReplicaIdType>();
                            TLogicalTimestamp logicalTimestamp = reader.Read<TLogicalTimestamp>();
                            TItemDependencyTimestamp itemDependencyTimestamp = reader.Read<TItemDependencyTimestamp>();
                            TItemDependencyReplica itemDependencyReplica = reader.Read<TItemDependencyReplica>();
                            TClientId clientId = reader.Read<TClientId>();
                            TClientDependencyTimestamp clientDependencyTimestamp = reader.Read<TClientDependencyTimestamp>();
                            TClientDependencyPartition clientDependencyPartition = reader.Read<TClientDependencyPartition>();
                            
                            return TSelf(key,
                                         value,
                                         timestamp,
                                         sourceReplicaId,
                                         logicalTimestamp,
                                         itemDependencyTimestamp,
                                         itemDependencyReplica,
                                         clientId,
                                         clientDependencyTimestamp,
                                         clientDependencyPartition);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType> TBufferWriter;
                            
                            const OrbeDelayedInstallMessageHeaderType bodySize = sizeof(m_messageType)
                            + sizeof(m_key)
                            + sizeof(m_value)
                            + sizeof(m_timestamp)
                            + sizeof(m_sourceReplicaId)
                            + sizeof(m_logicalTimestamp)
                            + sizeof(m_itemDependencyTimestamp)
                            + sizeof(m_itemDependencyReplica)
                            + sizeof(m_clientId)
                            + sizeof(m_clientDependencyTimestamp)
                            + sizeof(m_clientDependencyPartition);
                            
                            TBufferWriter writer(buffer, bodySize);
                            
                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key)
                            .template Write<TClientInputValueType>(m_value)
                            .template Write<uint64_t>(m_timestamp.count())
                            .template Write<ReplicaIdType>(m_sourceReplicaId)
                            .template Write<TLogicalTimestamp>(m_logicalTimestamp)
                            .template Write<TItemDependencyTimestamp>(m_itemDependencyTimestamp)
                            .template Write<TItemDependencyReplica>(m_itemDependencyReplica)
                            .template Write<TClientId>(m_clientId)
                            .template Write<TClientDependencyTimestamp>(m_clientDependencyTimestamp)
                            .template Write<TClientDependencyPartition>(m_clientDependencyPartition);
                        }
                        
                    private:
                        OrbeDelayedInstallReplicate(TKeyType key,
                                                    TClientInputValueType value,
                                                    TTimestamp timestamp,
                                                    ReplicaIdType sourceReplicaId,
                                                    TLogicalTimestamp logicalTimestamp,
                                                    TItemDependencyTimestamp itemDependencyTimestamp,
                                                    TItemDependencyReplica itemDependencyReplica,
                                                    TClientId clientId,
                                                    TClientDependencyTimestamp clientDependencyTimestamp,
                                                    TClientDependencyPartition clientDependencyPartition)
                        : m_messageType(Wire::ReplicateMsg)
                        , m_key(key)
                        , m_value(value)
                        , m_timestamp(timestamp)
                        , m_sourceReplicaId(sourceReplicaId)
                        , m_logicalTimestamp(logicalTimestamp)
                        , m_itemDependencyTimestamp(itemDependencyTimestamp)
                        , m_itemDependencyReplica(itemDependencyReplica)
                        , m_clientId(clientId)
                        , m_clientDependencyTimestamp(clientDependencyTimestamp)
                        , m_clientDependencyPartition(clientDependencyPartition)
                        {
                            
                        }
                        
                        OrbeDelayedInstallWireMessages m_messageType;
                        TKeyType m_key;
                        TClientInputValueType m_value;
                        TTimestamp m_timestamp;
                        ReplicaIdType m_sourceReplicaId;
                        TLogicalTimestamp m_logicalTimestamp;
                        TItemDependencyTimestamp m_itemDependencyTimestamp;
                        TItemDependencyReplica m_itemDependencyReplica;
                        TClientId m_clientId;
                        TClientDependencyTimestamp m_clientDependencyTimestamp;
                        TClientDependencyPartition m_clientDependencyPartition;
                    };
                }
            }
        }
    }
}


