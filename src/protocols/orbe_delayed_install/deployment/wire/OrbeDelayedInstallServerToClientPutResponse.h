//
//  OrbeDelayedInstallServerToClientPutResponse.h
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
                    class OrbeDelayedInstallServerToClientPutResponse
                    {
                        typedef OrbeDelayedInstallServerToClientPutResponse TSelf;
                        
                    public:
                        static TSelf Create()
                        {
                            return TSelf();
                        }
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }                        
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeDelayedInstallMessagePreamble> reader(buffer);
                            return TSelf();
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType);
                        }
                        
                    private:
                        OrbeDelayedInstallServerToClientPutResponse()
                        : m_messageType(Wire::ServerToClientPutResponseMsg)
                        {
                            
                        }
                        
                        OrbeDelayedInstallWireMessages m_messageType;
                        
                        static constexpr OrbeDelayedInstallMessageHeaderType BodySize = sizeof(m_messageType);
                    };
                }
            }
        }
    }
}
