//
//  OrbeDelayedInstallServerToClientGetResponse.h
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
                    class OrbeDelayedInstallServerToClientGetResponse
                    {
                        typedef OrbeDelayedInstallServerToClientGetResponse TSelf;
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
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }
                        
                        Utilities::Byte FoundValue() const { return m_foundValue; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeDelayedInstallMessagePreamble> reader(buffer);
                            Utilities::Byte foundValue = reader.Read<Utilities::Byte>() != 0;
                            TClientInputValueType value = reader.Read<TClientInputValueType>();
                            return TSelf(foundValue, value);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<Utilities::Byte>(m_foundValue)
                            .template Write<TClientInputValueType>(m_value);
                        }
                        
                    private:
                        OrbeDelayedInstallServerToClientGetResponse(Utilities::Byte foundValue, TClientInputValueType value)
                        : m_messageType(Wire::ServerToClientGetResponseMsg)
                        , m_foundValue(foundValue)
                        , m_value(value)
                        {
                            
                        }
                        
                        OrbeDelayedInstallWireMessages m_messageType;
                        Utilities::Byte m_foundValue;
                        TClientInputValueType m_value;
                        
                        static constexpr OrbeDelayedInstallMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_foundValue)
                        + sizeof(m_value);
                    };
                }
            }
        }
    }
}

