//
//  OrbeClientToServerPutRequest.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Orbe.h"
#include "OrbeWire.h"
#include "Wire.h"

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
                    class OrbeClientToServerPutRequest
                    {
                        typedef OrbeClientToServerPutRequest TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        
                    public:
                        static TSelf Create(TKeyType key,
                                            TClientInputValueType value)
                        {
                            return TSelf(key, value);
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeMessagePreamble> reader(buffer);
                            TKeyType key = reader.Read<TKeyType>();
                            TClientInputValueType value = reader.Read<TClientInputValueType>();
                            return TSelf(key, value);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key)
                            .template Write<TClientInputValueType>(m_value);
                        }
                        
                    private:
                        OrbeClientToServerPutRequest(TKeyType key,
                                                   TClientInputValueType value)
                        : m_messageType(Wire::ClientToServerPutRequestMsg)
                        , m_key(key)
                        , m_value(value)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        TKeyType m_key;
                        TClientInputValueType m_value;
                        
                        static constexpr OrbeMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_key)
                        + sizeof(m_value);
                    };
                }
            }
        }
    }
}
