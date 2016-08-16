//
//  OrbePartitionIdentityExchange.h
//  SimRunner
//
//  Created by Scott on 21/10/2014.
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
                    class OrbePartitionIdentityExchange
                    {
                        typedef OrbePartitionIdentityExchange TSelf;
                        typedef SimRunner::Protocols::PartitionIdType PartitionIdType;
                        
                    public:
                        static TSelf Create(PartitionIdType partitionId)
                        {
                            return TSelf(partitionId);
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }
                        
                        PartitionIdType PartitionId() const { return m_partitionId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeMessagePreamble> reader(buffer);
                            PartitionIdType partitionId = reader.Read<PartitionIdType>();
                            return TSelf(partitionId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<PartitionIdType>(m_partitionId);
                        }
                        
                    private:
                        OrbePartitionIdentityExchange(PartitionIdType partitionId)
                        : m_messageType(Wire::PartitionIdentityExchangeMsg)
                        , m_partitionId(partitionId)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        PartitionIdType m_partitionId;
                        
                        static constexpr OrbeMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_partitionId);
                    };
                }
            }
        }
    }
}
