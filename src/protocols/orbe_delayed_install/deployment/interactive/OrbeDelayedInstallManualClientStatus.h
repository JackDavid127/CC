//
//  OrbeDelayedInstallManualClientStatus.h
//  SimRunner
//
//  Created by Scott on 10/01/2015.
//  Copyright (c) 2015 Scott. All rights reserved.
//

#pragma once

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
                    class OrbeDelayedInstallManualClientStatus
                    {
                        typedef OrbeDelayedInstallManualClientStatus TSelf;
                        
                    public:
                        static TSelf Create()
                        {
                            return TSelf();
                        }
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            return TSelf();
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType> TBufferWriter;
                            TBufferWriter writer(buffer, BodySize);
                            writer.template Write<Utilities::Byte>(m_messageType);
                        }
                        
                    private:
                        OrbeDelayedInstallManualClientStatus()
                        : m_messageType(Wire::ManualClientStatusMsg)
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
