//
//  OrbeServerToClientPutResponse.h
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
                    class OrbeServerToClientPutResponse
                    {
                        typedef OrbeServerToClientPutResponse TSelf;
                        
                    public:
                        static TSelf Create()
                        {
                            return TSelf();
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }                        
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeMessagePreamble> reader(buffer);
                            return TSelf();
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType);
                        }
                        
                    private:
                        OrbeServerToClientPutResponse()
                        : m_messageType(Wire::ServerToClientPutResponseMsg)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        
                        static constexpr OrbeMessageHeaderType BodySize = sizeof(m_messageType);
                    };
                }
            }
        }
    }
}
