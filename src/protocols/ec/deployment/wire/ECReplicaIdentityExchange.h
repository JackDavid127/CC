//
//  ECReplicaIdentityExchange.h
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
                    class ECReplicaIdentityExchange
                    {
                        typedef ECReplicaIdentityExchange TSelf;
                        typedef SimRunner::Protocols::ReplicaIdType ReplicaIdType;
                        
                    public:
                        static TSelf Create(ReplicaIdType replicaIdType)
                        {
                            return TSelf(replicaIdType);
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }
                        
                        ReplicaIdType ReplicaId() const { return m_replicaId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            ReplicaIdType replicaId = reader.Read<ReplicaIdType>();
                            return TSelf(replicaId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<ECMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<ReplicaIdType>(m_replicaId);
                        }
                        
                    private:
                        ECReplicaIdentityExchange(ReplicaIdType replicaId)
                        : m_messageType(Wire::ReplicaIdentityExchangeMsg)
                        , m_replicaId(replicaId)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        ReplicaIdType m_replicaId;
                        
                        static constexpr ECMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_replicaId);
                    };
                }
            }
        }
    }
}
