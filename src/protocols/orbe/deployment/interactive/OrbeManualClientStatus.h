//
//  OrbeManualClientStatus.h
//  SimRunner
//
//  Created by Scott on 13/01/2015.
//  Copyright (c) 2015 Scott. All rights reserved.
//

#pragma once

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
                    class OrbeManualClientStatus
                    {
                        typedef OrbeManualClientStatus TSelf;
                        
                    public:
                        static TSelf Create()
                        {
                            return TSelf();
                        }
                        
                        OrbeManualClientStatus MessageType() const { return m_messageType; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            return TSelf();
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<TSelf> TBufferWriter;
                            TBufferWriter writer(buffer, BodySize);
                            writer.template Write<Utilities::Byte>(m_messageType);
                        }
                        
                    private:
                        OrbeManualClientStatus()
                        : m_messageType(Wire::ManualClientStatusMsg)
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
