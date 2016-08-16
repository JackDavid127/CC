//
//  OrbeServerToServerPutRequest.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Orbe.h"
#include "OrbeWire.h"
#include "Wire.h"
#include "OrbeDependencyMatrixParser.h"

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
                    class OrbeServerToServerPutRequest
                    {
                        typedef OrbeServerToServerPutRequest TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef SimRunner::Protocols::ClientIdType ClientIdType;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        typedef typename ProtocolTraits::TDependencyMatrix TDependencyMatrix;
                        
                    public:
                        static TSelf Create(TKeyType key, ClientIdType clientId, TClientInputValueType value, TDependencyMatrix dependencyMatrix)
                        {
                            return TSelf(key, clientId, value, dependencyMatrix);
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        ClientIdType ClientId() const { return m_clientId; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        TDependencyMatrix DependencyMatrix() const { return m_dependencyMatrix; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            TBufferReader reader(buffer);
                            TKeyType key = reader.Read<TKeyType>();
                            ClientIdType clientId = reader.Read<ClientIdType>();
                            TClientInputValueType value = reader.Read<TClientInputValueType>();
                            TDependencyMatrix dependencyMatrix(ReadDependencyMatrix<ProtocolTraits, TBufferReader>(reader));
                            return TSelf(key, clientId, value, dependencyMatrix);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<OrbeMessageHeaderType> TBufferWriter;
                            
                            const OrbeMessageHeaderType bodySize = sizeof(m_messageType)
                            + sizeof(m_key)
                            + sizeof(m_clientId)
                            + sizeof(m_value)
                            + static_cast<OrbeMessageHeaderType>(ComputeDependencyMatrixSerializationSize<ProtocolTraits>(m_dependencyMatrix));
                            
                            TBufferWriter writer(buffer, bodySize);

                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key)
                            .template Write<ClientIdType>(m_clientId)
                            .template Write<TClientInputValueType>(m_value);
                            
                            WriteDependencyMatrix<ProtocolTraits, TBufferWriter>(m_dependencyMatrix, writer);
                        }
                        
                    private:
                        OrbeServerToServerPutRequest(TKeyType key,
                                                     ClientIdType clientId,
                                                     TClientInputValueType value,
                                                     const TDependencyMatrix& dependencyMatrix)
                        : m_messageType(Wire::ServerToServerPutRequestMsg)
                        , m_key(key)
                        , m_clientId(clientId)
                        , m_value(value)
                        , m_dependencyMatrix(dependencyMatrix)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        TKeyType m_key;
                        ClientIdType m_clientId;
                        TClientInputValueType m_value;
                        TDependencyMatrix m_dependencyMatrix;
                    };
                }
            }
        }
    }
}
