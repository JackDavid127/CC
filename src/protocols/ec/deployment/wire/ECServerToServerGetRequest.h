//
//  ECServerToServerGetRequest.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "EC.h"
#include "ECWire.h"
#include "Wire.h"

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
                    class ECServerToServerGetRequest
                    {
                        typedef ECServerToServerGetRequest TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef SimRunner::Protocols::ClientIdType ClientIdType;
                        
                    public:
                        static TSelf Create(TKeyType key, ClientIdType clientId)
                        {
                            return TSelf(key, clientId);
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        ClientIdType ClientId() const { return m_clientId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            TKeyType key = reader.Read<TKeyType>();
                            ClientIdType clientId = reader.Read<ClientIdType>();
                            return TSelf(key, clientId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<ECMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key)
                            .template Write<ClientIdType>(m_clientId);
                        }
                        
                    private:
                        ECServerToServerGetRequest(TKeyType key, ClientIdType clientId)
                        : m_messageType(Wire::ServerToServerGetRequestMsg)
                        , m_key(key)
                        , m_clientId(clientId)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        TKeyType m_key;
                        ClientIdType m_clientId;
                        
                        static constexpr ECMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_key)
                        + sizeof(m_clientId);
                    };
                }
            }
        }
    }
}
