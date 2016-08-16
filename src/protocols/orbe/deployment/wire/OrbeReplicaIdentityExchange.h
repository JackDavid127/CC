//
//  OrbeReplicaIdentityExchange.h
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
                    class OrbeReplicaIdentityExchange
                    {
                        typedef OrbeReplicaIdentityExchange TSelf;
                        typedef SimRunner::Protocols::ReplicaIdType ReplicaIdType;
                        
                    public:
                        static TSelf Create(ReplicaIdType replicaIdType)
                        {
                            return TSelf(replicaIdType);
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }
                        
                        ReplicaIdType ReplicaId() const { return m_replicaId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeMessagePreamble> reader(buffer);
                            ReplicaIdType replicaId = reader.Read<ReplicaIdType>();
                            return TSelf(replicaId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<ReplicaIdType>(m_replicaId);
                        }
                        
                    private:
                        OrbeReplicaIdentityExchange(ReplicaIdType replicaId)
                        : m_messageType(Wire::ReplicaIdentityExchangeMsg)
                        , m_replicaId(replicaId)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        ReplicaIdType m_replicaId;
                        
                        static constexpr OrbeMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_replicaId);
                    };
                }
            }
        }
    }
}
