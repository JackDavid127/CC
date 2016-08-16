//
//  OrbeDelayedInstallReplicaHeartbeat.h
//  SimRunner
//
//  Created by Scott on 10/01/2015.
//  Copyright (c) 2015 Scott. All rights reserved.
//

#pragma once

#include "OrbeDelayedInstall.h"
#include "OrbeDelayedInstallWire.h"
#include "OrbeVersionVectorParser.h"

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
                    class OrbeDelayedInstallReplicaHeartbeat
                    {
                        typedef OrbeDelayedInstallReplicaHeartbeat TSelf;
                        typedef typename ProtocolTraits::TVersionVector TVersionVector;
                        
                    public:
                        static TSelf Create(PartitionIdType senderPartitionId,
                                            ReplicaIdType sourceReplicaId,
                                            const TVersionVector& dependencyVersionVector)
                        {
                            return TSelf(senderPartitionId, sourceReplicaId, dependencyVersionVector);
                        }
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }
                        
                        PartitionIdType SenderPartitionId() const { return m_senderPartitionId; }
                        
                        ReplicaIdType SourceReplicaId() const { return m_sourceReplicaId; }
                        
                        const TVersionVector& DependencyVersionVector() const { return m_dependencyVersionVector; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            TBufferReader reader(buffer);
                            
                            PartitionIdType senderPartitionId = reader.Read<PartitionIdType>();
                            ReplicaIdType sourceReplicaId = reader.Read<ReplicaIdType>();
                            TVersionVector dependencyVersionVector(Orbe::Deployment::Wire::ReadVersionVector<ProtocolTraits, TBufferReader>(reader));
                            
                            return TSelf(senderPartitionId, sourceReplicaId, dependencyVersionVector);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType> TBufferWriter;
                            TBufferWriter writer(buffer, BodySize);
                            
                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<PartitionIdType>(m_senderPartitionId)
                            .template Write<ReplicaIdType>(m_sourceReplicaId);
                            
                            Orbe::Deployment::Wire::WriteVersionVector<ProtocolTraits, TBufferWriter>(m_dependencyVersionVector, writer);
                        }
                        
                    private:
                        OrbeDelayedInstallReplicaHeartbeat(PartitionIdType senderPartitionId,
                                                           ReplicaIdType sourceReplicaId,
                                                           const TVersionVector& dependencyVersionVector)
                        : m_messageType(Wire::ReplicaHeartbeatMsg)
                        , m_senderPartitionId(senderPartitionId)
                        , m_sourceReplicaId(sourceReplicaId)
                        , m_dependencyVersionVector(dependencyVersionVector)
                        {
                            
                        }
                        
                        OrbeDelayedInstallWireMessages m_messageType;
                        PartitionIdType m_senderPartitionId;
                        ReplicaIdType m_sourceReplicaId;
                        TVersionVector m_dependencyVersionVector;
                        
                        static constexpr OrbeDelayedInstallMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_senderPartitionId)
                        + sizeof(m_sourceReplicaId)
                        + TVersionVector::SerializationSize();
                    };
                }
            }
        }
    }
}
