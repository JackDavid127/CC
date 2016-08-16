//
//  BoltOnServerToClientPutResponse.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "BoltOn.h"
#include "BoltOnWire.h"
#include "Wire.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace Deployment
            {
                namespace Wire
                {
                    template <typename ProtocolTraits>
                    class BoltOnServerToClientPutResponse
                    {
                        typedef BoltOnServerToClientPutResponse TSelf;
                        
                    public:
                        static TSelf Create()
                        {
                            return TSelf();
                        }
                        
                        BoltOnWireMessages MessageType() const { return m_messageType; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<BoltOnMessagePreamble> reader(buffer);
                            return TSelf();
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<BoltOnMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType);
                        }
                        
                    private:
                        BoltOnServerToClientPutResponse()
                        : m_messageType(Wire::ServerToClientPutResponseMsg)
                        {
                            
                        }
                        
                        BoltOnWireMessages m_messageType;
                        
                        static constexpr BoltOnMessageHeaderType BodySize = sizeof(m_messageType);
                    };
                }
            }
        }
    }
}
