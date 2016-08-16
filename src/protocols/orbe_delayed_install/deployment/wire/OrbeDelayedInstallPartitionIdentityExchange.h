//
//  OrbeDelayedInstallPartitionIdentityExchange.h
//  SimRunner
//
//  Created by Scott on 21/10/2014.
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
                    class OrbeDelayedInstallPartitionIdentityExchange
                    {
                        typedef OrbeDelayedInstallPartitionIdentityExchange TSelf;
                        typedef SimRunner::Protocols::PartitionIdType PartitionIdType;
                        
                    public:
                        static TSelf Create(PartitionIdType partitionId)
                        {
                            return TSelf(partitionId);
                        }
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }
                        
                        PartitionIdType PartitionId() const { return m_partitionId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeDelayedInstallMessagePreamble> reader(buffer);
                            PartitionIdType partitionId = reader.Read<PartitionIdType>();
                            return TSelf(partitionId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<PartitionIdType>(m_partitionId);
                        }
                        
                    private:
                        OrbeDelayedInstallPartitionIdentityExchange(PartitionIdType partitionId)
                        : m_messageType(Wire::PartitionIdentityExchangeMsg)
                        , m_partitionId(partitionId)
                        {
                            
                        }
                        
                        OrbeDelayedInstallWireMessages m_messageType;
                        PartitionIdType m_partitionId;
                        
                        static constexpr OrbeDelayedInstallMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_partitionId);
                    };
                }
            }
        }
    }
}
