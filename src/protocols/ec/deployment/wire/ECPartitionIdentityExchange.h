//
//  ECPartitionIdentityExchange.h
//  SimRunner
//
//  Created by Scott on 21/10/2014.
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
                    class ECPartitionIdentityExchange
                    {
                        typedef ECPartitionIdentityExchange TSelf;
                        typedef SimRunner::Protocols::PartitionIdType PartitionIdType;
                        
                    public:
                        static TSelf Create(PartitionIdType partitionId)
                        {
                            return TSelf(partitionId);
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }
                        
                        PartitionIdType PartitionId() const { return m_partitionId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            PartitionIdType partitionId = reader.Read<PartitionIdType>();
                            return TSelf(partitionId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<ECMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<PartitionIdType>(m_partitionId);
                        }
                        
                    private:
                        ECPartitionIdentityExchange(PartitionIdType partitionId)
                        : m_messageType(Wire::PartitionIdentityExchangeMsg)
                        , m_partitionId(partitionId)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        PartitionIdType m_partitionId;
                        
                        static constexpr ECMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_partitionId);
                    };
                }
            }
        }
    }
}
