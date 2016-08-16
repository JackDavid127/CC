//
//  ECReplicate.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "EC.h"
#include "ECWire.h"
#include "Wire.h"
#include "Timing.h"
#include "StringValueParser.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            namespace Deployment
            {
                namespace Wire
                {
                    template <typename ProtocolTraits>
                    class ECReplicate
                    {
                        typedef ECReplicate TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        typedef Utilities::TTimestamp TTimestamp;
                        typedef SimRunner::Protocols::ReplicaIdType ReplicaIdType;
                        
                    public:
                        static TSelf Create(TKeyType key,
                                            TClientInputValueType value,
                                            TTimestamp timestamp,
                                            ReplicaIdType replicaId)
                        {
                            return TSelf(key, value, timestamp, replicaId);
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        TTimestamp Timestamp() const { return m_timestamp; }
                        
                        ReplicaIdType ReplicaId() const { return m_replicaId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            TKeyType key = reader.Read<TKeyType>();
                            TClientInputValueType value(ReadString(reader));
                            TTimestamp timestamp(reader.Read<uint64_t>());
                            ReplicaIdType replica = reader.Read<ReplicaIdType>();
                            return TSelf(key, value, timestamp, replica);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<ECMessageHeaderType> TBufferWriter;
                            
                            const ECMessageHeaderType BodySize = sizeof(m_messageType)
                            + sizeof(m_key)
                            + static_cast<ECMessageHeaderType>(Utilities::ComputeStringSerializationSize(m_value))
                            + sizeof(m_timestamp)
                            + sizeof(m_replicaId);
                            
                            TBufferWriter writer(buffer, BodySize);
                            
                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key);
                            
                            WriteString(m_value, writer);
                            
                            writer
                            .template Write<uint64_t>(m_timestamp.count())
                            .template Write<ReplicaIdType>(m_replicaId);
                        }
                        
                    private:
                        ECReplicate(TKeyType key,
                                    TClientInputValueType value,
                                    TTimestamp timestamp,
                                    ReplicaIdType replicaId)
                        : m_messageType(Wire::ReplicateMsg)
                        , m_key(key)
                        , m_value(value)
                        , m_timestamp(timestamp)
                        , m_replicaId(replicaId)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        TKeyType m_key;
                        TClientInputValueType m_value;
                        TTimestamp m_timestamp;
                        ReplicaIdType m_replicaId;
                        
                        static constexpr ECMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_key)
                        + sizeof(m_value)
                        + sizeof(m_timestamp)
                        + sizeof(m_replicaId);
                    };
                }
            }
        }
    }
}


