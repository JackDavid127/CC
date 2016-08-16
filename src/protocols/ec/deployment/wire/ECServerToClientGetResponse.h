//
//  ECServerToClientGetResponse.h
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
                    class ECServerToClientGetResponse
                    {
                        typedef ECServerToClientGetResponse TSelf;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        
                    public:
                        static TSelf CreateFound(TClientInputValueType value)
                        {
                            return TSelf(Utilities::Byte(1), value);
                        }
                        
                        static TSelf CreateNotFound()
                        {
                            return TSelf(Utilities::Byte(0), TClientInputValueType());
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }
                        
                        Utilities::Byte FoundValue() const { return m_foundValue; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            Utilities::Byte foundValue = reader.Read<Utilities::Byte>() != 0;
                            TClientInputValueType value(ReadString(reader));
                            return TSelf(foundValue, value);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<ECMessageHeaderType> TBufferWriter;
                            
                            const ECMessageHeaderType BodySize = sizeof(m_messageType)
                            + sizeof(m_foundValue)
                            + static_cast<ECMessageHeaderType>(Utilities::ComputeStringSerializationSize(m_value));
                            
                            TBufferWriter writer(buffer, BodySize);
                            
                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<Utilities::Byte>(m_foundValue);
                            
                            WriteString(m_value, writer);
                        }
                        
                    private:
                        ECServerToClientGetResponse(Utilities::Byte foundValue, TClientInputValueType value)
                        : m_messageType(Wire::ServerToClientGetResponseMsg)
                        , m_foundValue(foundValue)
                        , m_value(value)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        Utilities::Byte m_foundValue;
                        TClientInputValueType m_value;
                    };
                }
            }
        }
    }
}

