//
//  OrbeDelayedInstallServerToServerPutRequest.h
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
                    class OrbeDelayedInstallServerToServerPutRequest
                    {
                        typedef OrbeDelayedInstallServerToServerPutRequest TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef SimRunner::Protocols::ClientIdType ClientIdType;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        typedef typename ProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                        typedef typename ProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                        
                    public:
                        static TSelf Create(TKeyType key,
                                            ClientIdType clientId,
                                            TClientInputValueType value,
                                            TClientDependencyTimestamp clientDependencyTimestamp,
                                            TClientDependencyPartition clientDependencyPartition)
                        {
                            return TSelf(key, clientId, value, clientDependencyTimestamp, clientDependencyPartition);
                        }
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        ClientIdType ClientId() const { return m_clientId; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        TClientDependencyTimestamp ClientDependencyTimestamp() const { return m_clientDependencyTimestamp; }
                        
                        TClientDependencyPartition ClientDependencyPartition() const { return m_clientDependencyPartition; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            TBufferReader reader(buffer);
                            TKeyType key = reader.Read<TKeyType>();
                            ClientIdType clientId = reader.Read<ClientIdType>();
                            TClientInputValueType value = reader.Read<TClientInputValueType>();
                            TClientDependencyTimestamp clientDependencyTimestamp = reader.Read<TClientDependencyTimestamp>();
                            TClientDependencyPartition clientDependencyPartition = reader.Read<TClientDependencyPartition>();
                            return TSelf(key, clientId, value, clientDependencyTimestamp, clientDependencyPartition);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType> TBufferWriter;
                            
                            const OrbeDelayedInstallMessageHeaderType bodySize = sizeof(m_messageType)
                            + sizeof(m_key)
                            + sizeof(m_clientId)
                            + sizeof(m_value)
                            + sizeof(m_clientDependencyTimestamp)
                            + sizeof(m_clientDependencyPartition);
                            
                            TBufferWriter writer(buffer, bodySize);

                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key)
                            .template Write<ClientIdType>(m_clientId)
                            .template Write<TClientInputValueType>(m_value)
                            .template Write<TClientDependencyTimestamp>(m_clientDependencyTimestamp)
                            .template Write<TClientDependencyPartition>(m_clientDependencyPartition);
                        }
                        
                    private:
                        OrbeDelayedInstallServerToServerPutRequest(TKeyType key,
                                                                   ClientIdType clientId,
                                                                   TClientInputValueType value,
                                                                   TClientDependencyTimestamp clientDependencyTimestamp,
                                                                   TClientDependencyPartition clientDependencyPartition)
                        : m_messageType(Wire::ServerToServerPutRequestMsg)
                        , m_key(key)
                        , m_clientId(clientId)
                        , m_value(value)
                        , m_clientDependencyTimestamp(clientDependencyTimestamp)
                        , m_clientDependencyPartition(clientDependencyPartition)
                        {
                            
                        }
                        
                        OrbeDelayedInstallWireMessages m_messageType;
                        TKeyType m_key;
                        ClientIdType m_clientId;
                        TClientInputValueType m_value;
                        TClientDependencyTimestamp m_clientDependencyTimestamp;
                        TClientDependencyPartition m_clientDependencyPartition;
                    };
                }
            }
        }
    }
}
