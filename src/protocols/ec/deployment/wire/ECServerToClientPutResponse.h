//
//  ECServerToClientPutResponse.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "EC.h"
#include "ECWire.h"
#include "Wire.h"

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
                    class ECServerToClientPutResponse
                    {
                        typedef ECServerToClientPutResponse TSelf;
                        
                    public:
                        static TSelf Create()
                        {
                            return TSelf();
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }                        
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            return TSelf();
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<ECMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType);
                        }
                        
                    private:
                        ECServerToClientPutResponse()
                        : m_messageType(Wire::ServerToClientPutResponseMsg)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        
                        static constexpr ECMessageHeaderType BodySize = sizeof(m_messageType);
                    };
                }
            }
        }
    }
}
