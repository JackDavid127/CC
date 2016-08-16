//
//  OrbeVersionVectorCheckRequest.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Orbe.h"
#include "OrbeWire.h"
#include "OrbeVersionVectorParser.h"

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
                    class OrbeVersionVectorCheckRequest
                    {
                        typedef OrbeVersionVectorCheckRequest TSelf;
                        typedef typename ProtocolTraits::TVersionVector TVersionVector;
                        
                    public:
                        static TSelf Create(PartitionIdType senderPartitionId,
                                            ReplicaIdType sourceReplicaId,
                                            const TVersionVector& dependencyVersionVector)
                        {
                            return TSelf(senderPartitionId, sourceReplicaId, dependencyVersionVector);
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }
                        
                        PartitionIdType SenderPartitionId() const { return m_senderPartitionId; }
                        
                        ReplicaIdType SourceReplicaId() const { return m_sourceReplicaId; }
                        
                        const TVersionVector& DependencyVersionVector() const { return m_dependencyVersionVector; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            TBufferReader reader(buffer);
                            PartitionIdType senderPartitionId = reader.Read<PartitionIdType>();
                            ReplicaIdType sourceReplicaId = reader.Read<ReplicaIdType>();
                            TVersionVector dependencyVersionVector(ReadVersionVector<ProtocolTraits, TBufferReader>(reader));
                            return TSelf(senderPartitionId, sourceReplicaId, dependencyVersionVector);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<OrbeMessageHeaderType> TBufferWriter;
                            TBufferWriter writer(buffer, BodySize);
                            
                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<PartitionIdType>(m_senderPartitionId)
                            .template Write<ReplicaIdType>(m_sourceReplicaId);
                            
                            WriteVersionVector<ProtocolTraits, TBufferWriter>(m_dependencyVersionVector, writer);
                        }
                        
                    private:
                        OrbeVersionVectorCheckRequest(PartitionIdType senderPartitionId,
                                                      ReplicaIdType sourceReplicaId,
                                                      const TVersionVector& dependencyVersionVector)
                        : m_messageType(Wire::VersionVectorCheckRequestMsg)
                        , m_senderPartitionId(senderPartitionId)
                        , m_sourceReplicaId(sourceReplicaId)
                        , m_dependencyVersionVector(dependencyVersionVector)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        PartitionIdType m_senderPartitionId;
                        ReplicaIdType m_sourceReplicaId;
                        TVersionVector m_dependencyVersionVector;
                        
                        static constexpr OrbeMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_senderPartitionId)
                        + sizeof(m_sourceReplicaId)
                        + TVersionVector::SerializationSize();
                    };
                }
            }
        }
    }
}
