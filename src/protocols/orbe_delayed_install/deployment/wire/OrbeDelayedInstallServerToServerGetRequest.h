//
//  OrbeDelayedInstallServerToServerGetRequest.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "OrbeDelayedInstall.h"
#include "OrbeDelayedInstallWire.h"
#include "Wire.h"

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
                    class OrbeDelayedInstallServerToServerGetRequest
                    {
                        typedef OrbeDelayedInstallServerToServerGetRequest TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef SimRunner::Protocols::ClientIdType ClientIdType;
                        
                    public:
                        static TSelf Create(TKeyType key, ClientIdType clientId)
                        {
                            return TSelf(key, clientId);
                        }
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        ClientIdType ClientId() const { return m_clientId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeDelayedInstallMessagePreamble> reader(buffer);
                            TKeyType key = reader.Read<TKeyType>();
                            ClientIdType clientId = reader.Read<ClientIdType>();
                            return TSelf(key, clientId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key)
                            .template Write<ClientIdType>(m_clientId);
                        }
                        
                    private:
                        OrbeDelayedInstallServerToServerGetRequest(TKeyType key, ClientIdType clientId)
                        : m_messageType(Wire::ServerToServerGetRequestMsg)
                        , m_key(key)
                        , m_clientId(clientId)
                        {
                            
                        }
                        
                        OrbeDelayedInstallWireMessages m_messageType;
                        TKeyType m_key;
                        ClientIdType m_clientId;
                        
                        static constexpr OrbeDelayedInstallMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_key)
                        + sizeof(m_clientId);
                    };
                }
            }
        }
    }
}
