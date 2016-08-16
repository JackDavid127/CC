//
//  ECServerToServerGetResponse.h
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
                    class ECServerToServerGetResponse
                    {
                        typedef ECServerToServerGetResponse TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        typedef Utilities::TTimestamp TTimestamp;
                        typedef SimRunner::Protocols::ClientIdType ClientIdType;
                        
                    public:
                        static TSelf Create(Utilities::Byte foundValue,
                                            TKeyType key,
                                            TClientInputValueType value,
                                            TTimestamp timestamp,
                                            ClientIdType clientId)
                        {
                            return TSelf(foundValue, key, value, timestamp, clientId);
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }
                        
                        Utilities::Byte FoundValue() const { return m_foundValue; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        TTimestamp Timestamp() const { return m_timestamp; }
                        
                        ClientIdType ClientId() const { return m_clientId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            Utilities::Byte foundValue = reader.Read<Utilities::Byte>();
                            TKeyType key = reader.Read<TKeyType>();
                            TClientInputValueType value(ReadString(reader));
                            TTimestamp timestamp(reader.Read<uint64_t>());
                            ClientIdType clientId = reader.Read<ClientIdType>();
                            return TSelf(foundValue, key, value, timestamp, clientId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<ECMessageHeaderType> TBufferWriter;
                            
                            const ECMessageHeaderType BodySize = sizeof(m_messageType)
                            + sizeof(m_foundValue)
                            + sizeof(m_key)
                            + static_cast<ECMessageHeaderType>(Utilities::ComputeStringSerializationSize(m_value))
                            + sizeof(m_timestamp)
                            + sizeof(m_clientId);
                            
                            TBufferWriter writer(buffer, BodySize);

                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<Utilities::Byte>(m_foundValue)
                            .template Write<TKeyType>(m_key);
                            
                            WriteString(m_value, writer);

                            writer
                            .template Write<uint64_t>(m_timestamp.count())
                            .template Write<ClientIdType>(m_clientId);
                        }
                        
                    private:
                        ECServerToServerGetResponse(Utilities::Byte foundValue,
                                                    TKeyType key,
                                                    TClientInputValueType value,
                                                    TTimestamp timestamp,
                                                    ClientIdType clientId)
                        : m_messageType(Wire::ServerToServerGetResponseMsg)
                        , m_foundValue(foundValue)
                        , m_key(key)
                        , m_value(value)
                        , m_timestamp(timestamp)
                        , m_clientId(clientId)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        Utilities::Byte m_foundValue;
                        TKeyType m_key;
                        TClientInputValueType m_value;
                        TTimestamp m_timestamp;
                        ClientIdType m_clientId;
                    };
                }
            }
        }
    }
}

