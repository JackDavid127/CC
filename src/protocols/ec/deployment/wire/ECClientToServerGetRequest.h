//
//  ECClientToServerGetRequest.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "EC.h"
#include "ECWire.h"
#include "Wire.h"
#include "Keys.h"

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
                    class ECClientToServerGetRequest
                    {
                        typedef ECClientToServerGetRequest TSelf;
                        typedef Keys::KeyType TKeyType;
                        
                    public:
                        static TSelf Create(TKeyType key)
                        {
                            return TSelf(key);
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            PartitionIdType key = reader.Read<TKeyType>();
                            return TSelf(key);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<ECMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key);
                        }
                        
                    private:
                        ECClientToServerGetRequest(TKeyType key)
                        : m_messageType(Wire::ClientToServerGetRequestMsg)
                        , m_key(key)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        TKeyType m_key;
                        
                        //todo -- do this automatically somehow
                        static constexpr ECMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_key);
                    };
                }
            }
        }
    }
}
