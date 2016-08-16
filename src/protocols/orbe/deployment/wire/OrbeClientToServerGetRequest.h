//
//  OrbeClientToServerGetRequest.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Orbe.h"
#include "OrbeWire.h"
#include "Wire.h"
#include "Keys.h"

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
                    class OrbeClientToServerGetRequest
                    {
                        typedef OrbeClientToServerGetRequest TSelf;
                        typedef Keys::KeyType TKeyType;
                        
                    public:
                        static TSelf Create(TKeyType key)
                        {
                            return TSelf(key);
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeMessagePreamble> reader(buffer);
                            PartitionIdType key = reader.Read<TKeyType>();
                            return TSelf(key);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key);
                        }
                        
                    private:
                        OrbeClientToServerGetRequest(TKeyType key)
                        : m_messageType(Wire::ClientToServerGetRequestMsg)
                        , m_key(key)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        TKeyType m_key;
                        
                        static constexpr OrbeMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_key);
                    };
                }
            }
        }
    }
}
