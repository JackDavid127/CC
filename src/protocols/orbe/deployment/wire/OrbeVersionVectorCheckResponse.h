//
//  OrbeVersionVectorCheckResponse.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Orbe.h"
#include "OrbeWire.h"

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
                    class OrbeVersionVectorCheckResponse
                    {
                        typedef OrbeVersionVectorCheckResponse TSelf;
                    public:
                        static TSelf Create(ReplicaIdType sourceReplicaId)
                        {
                            return TSelf(sourceReplicaId);
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }
                        
                        ReplicaIdType SourceReplicaId() const { return m_sourceReplicaId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeMessagePreamble> reader(buffer);
                            ReplicaIdType sourceReplicaId = reader.Read<ReplicaIdType>();
                            return TSelf(sourceReplicaId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<ReplicaIdType>(m_sourceReplicaId);
                        }
                        
                    private:
                        OrbeVersionVectorCheckResponse(ReplicaIdType sourceReplicaId)
                        : m_messageType(Wire::VersionVectorCheckResponseMsg)
                        , m_sourceReplicaId(sourceReplicaId)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        ReplicaIdType m_sourceReplicaId;
                        
                        static constexpr OrbeMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_sourceReplicaId);
                    };
                }
            }
        }
    }
}
