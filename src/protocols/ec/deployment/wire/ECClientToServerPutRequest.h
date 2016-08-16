//
//  ECClientToServerPutRequest.h
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
                    class ECClientToServerPutRequest
                    {
                        typedef ECClientToServerPutRequest TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        
                    public:
                        static TSelf Create(TKeyType key,
                                            TClientInputValueType value)
                        {
                            return TSelf(key, value);
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            TKeyType key = reader.Read<TKeyType>();
                            TClientInputValueType value(ReadString(reader));
                            return TSelf(key, value);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<ECMessageHeaderType> TBufferWriter;
                            
                            const ECMessageHeaderType BodySize = sizeof(m_messageType)
                            + sizeof(m_key)
                            + static_cast<ECMessageHeaderType>(Utilities::ComputeStringSerializationSize(m_value));
                            
                            TBufferWriter writer(buffer, BodySize);
                            
                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key);
                            
                            WriteString(m_value, writer);
                        }
                        
                    private:
                        ECClientToServerPutRequest(TKeyType key,
                                                   TClientInputValueType value)
                        : m_messageType(Wire::ClientToServerPutRequestMsg)
                        , m_key(key)
                        , m_value(value)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        TKeyType m_key;
                        TClientInputValueType m_value;
                    };
                }
            }
        }
    }
}
