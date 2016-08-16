//
//  OrbeServerToServerGetResponse.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Orbe.h"
#include "OrbeWire.h"
#include "Wire.h"
#include "Timing.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace Deployment
            {
                namespace Wire
                {
                    template <typename ProtocolTraits>
                    class OrbeServerToServerGetResponse
                    {
                        typedef OrbeServerToServerGetResponse TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        typedef Utilities::TTimestamp TTimestamp;
                        typedef SimRunner::Protocols::ClientIdType ClientIdType;
                        typedef SimRunner::Protocols::ReplicaIdType ReplicaIdType;
                        typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                        
                    public:
                        static TSelf Create(Utilities::Byte foundValue,
                                            TKeyType key,
                                            TClientInputValueType value,
                                            TTimestamp timestamp,
                                            ClientIdType clientId,
                                            ReplicaIdType sourceReplicaId,
                                            TLogicalTimestamp logicalTimestamp)
                        {
                            return TSelf(foundValue, key, value, timestamp, clientId, sourceReplicaId, logicalTimestamp);
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }
                        
                        Utilities::Byte FoundValue() const { return m_foundValue; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        TTimestamp Timestamp() const { return m_timestamp; }
                        
                        ClientIdType ClientId() const { return m_clientId; }
                        
                        ReplicaIdType SourceReplicaId() const { return m_sourceReplicaId; }
                        
                        TLogicalTimestamp LogicalTimestamp() const { return m_logicalTimestamp; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeMessagePreamble> reader(buffer);
                            Utilities::Byte foundValue = reader.Read<Utilities::Byte>();
                            TKeyType key = reader.Read<TKeyType>();
                            TClientInputValueType value = reader.Read<TClientInputValueType>();
                            TTimestamp timestamp(reader.Read<uint64_t>());
                            ClientIdType clientId = reader.Read<ClientIdType>();
                            ReplicaIdType sourceReplicaId = reader.Read<ReplicaIdType>();
                            TLogicalTimestamp logicalTimestamp = reader.Read<TLogicalTimestamp>();
                            return TSelf(foundValue, key, value, timestamp, clientId, sourceReplicaId, logicalTimestamp);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<Utilities::Byte>(m_foundValue)
                            .template Write<TKeyType>(m_key)
                            .template Write<TClientInputValueType>(m_value)
                            .template Write<uint64_t>(m_timestamp.count())
                            .template Write<ClientIdType>(m_clientId)
                            .template Write<ReplicaIdType>(m_sourceReplicaId)
                            .template Write<TLogicalTimestamp>(m_logicalTimestamp);
                        }
                        
                    private:
                        OrbeServerToServerGetResponse(Utilities::Byte foundValue,
                                                      TKeyType key,
                                                      TClientInputValueType value,
                                                      TTimestamp timestamp,
                                                      ClientIdType clientId,
                                                      ReplicaIdType sourceReplicaId,
                                                      TLogicalTimestamp logicalTimestamp)
                        : m_messageType(Wire::ServerToServerGetResponseMsg)
                        , m_foundValue(foundValue)
                        , m_key(key)
                        , m_value(value)
                        , m_timestamp(timestamp)
                        , m_clientId(clientId)
                        , m_sourceReplicaId(sourceReplicaId)
                        , m_logicalTimestamp(logicalTimestamp)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        Utilities::Byte m_foundValue;
                        TKeyType m_key;
                        TClientInputValueType m_value;
                        TTimestamp m_timestamp;
                        ClientIdType m_clientId;
                        ReplicaIdType m_sourceReplicaId;
                        TLogicalTimestamp m_logicalTimestamp;
                        
                        static constexpr OrbeMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_foundValue)
                        + sizeof(m_key)
                        + sizeof(m_value)
                        + sizeof(m_timestamp)
                        + sizeof(m_clientId)
                        + sizeof(m_sourceReplicaId)
                        + sizeof(m_logicalTimestamp);
                    };
                }
            }
        }
    }
}

