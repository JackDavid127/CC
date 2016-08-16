//
//  ECServerToServerPutRequest.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "EC.h"
#include "ECWire.h"
#include "Wire.h"
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
                    class ECServerToServerPutRequest
                    {
                        typedef ECServerToServerPutRequest TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef SimRunner::Protocols::ClientIdType ClientIdType;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        
                    public:
                        static TSelf Create(TKeyType key, ClientIdType clientId, TClientInputValueType value)
                        {
                            return TSelf(key, clientId, value);
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        ClientIdType ClientId() const { return m_clientId; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            TKeyType key = reader.Read<TKeyType>();
                            ClientIdType clientId = reader.Read<ClientIdType>();
                            TClientInputValueType value(ReadString(reader));
                            return TSelf(key, clientId, value);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<ECMessageHeaderType> TBufferWriter;
                            
                            const ECMessageHeaderType BodySize = sizeof(m_messageType)
                            + sizeof(m_key)
                            + sizeof(m_clientId)
                            + sizeof(m_value)
                            + static_cast<ECMessageHeaderType>(Utilities::ComputeStringSerializationSize(m_value));
                            
                            TBufferWriter writer(buffer, BodySize);
                            
                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key)
                            .template Write<ClientIdType>(m_clientId);
                            
                            WriteString(m_value, writer);
                        }
                        
                    private:
                        ECServerToServerPutRequest(TKeyType key, ClientIdType clientId, TClientInputValueType value)
                        : m_messageType(Wire::ServerToServerPutRequestMsg)
                        , m_key(key)
                        , m_clientId(clientId)
                        , m_value(value)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        TKeyType m_key;
                        ClientIdType m_clientId;
                        TClientInputValueType m_value;
                    };
                }
            }
        }
    }
}
