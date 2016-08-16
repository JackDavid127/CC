//
//  OrbeDelayedInstallReplicaIdentityExchange.h
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
                    class OrbeDelayedInstallReplicaIdentityExchange
                    {
                        typedef OrbeDelayedInstallReplicaIdentityExchange TSelf;
                        typedef SimRunner::Protocols::ReplicaIdType ReplicaIdType;
                        
                    public:
                        static TSelf Create(ReplicaIdType replicaIdType)
                        {
                            return TSelf(replicaIdType);
                        }
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }
                        
                        ReplicaIdType ReplicaId() const { return m_replicaId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeDelayedInstallMessagePreamble> reader(buffer);
                            ReplicaIdType replicaId = reader.Read<ReplicaIdType>();
                            return TSelf(replicaId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<ReplicaIdType>(m_replicaId);
                        }
                        
                    private:
                        OrbeDelayedInstallReplicaIdentityExchange(ReplicaIdType replicaId)
                        : m_messageType(Wire::ReplicaIdentityExchangeMsg)
                        , m_replicaId(replicaId)
                        {
                            
                        }
                        
                        OrbeDelayedInstallWireMessages m_messageType;
                        ReplicaIdType m_replicaId;
                        
                        static constexpr OrbeDelayedInstallMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_replicaId);
                    };
                }
            }
        }
    }
}
