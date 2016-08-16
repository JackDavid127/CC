//
//  OrbeServerToServerPutResponse.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
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
                    class OrbeServerToServerPutResponse
                    {
                        typedef OrbeServerToServerPutResponse TSelf;
                        typedef Utilities::TTimestamp TTimestamp;
                        typedef SimRunner::Protocols::ClientIdType ClientIdType;
                        typedef SimRunner::Protocols::ReplicaIdType ReplicaIdType;
                        typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                        
                    public:
                        static TSelf Create(TTimestamp timestamp,
                                            ClientIdType clientId,
                                            ReplicaIdType sourceReplicaId,
                                            TLogicalTimestamp logicalTimestamp)
                        {
                            return TSelf(timestamp, clientId, sourceReplicaId, logicalTimestamp);
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }
                        
                        TTimestamp Timestamp() const { return m_timestamp; }
                        
                        ClientIdType ClientId() const { return m_clientId; }
                        
                        ReplicaIdType SourceReplicaId() const { return m_sourceReplicaId; }
                        
                        TLogicalTimestamp LogicalTimestamp() const { return m_logicalTimestamp; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeMessagePreamble> reader(buffer);
                            TTimestamp timestamp(reader.Read<uint64_t>());
                            ClientIdType clientId = reader.Read<ClientIdType>();
                            ReplicaIdType sourceReplicaId = reader.Read<ReplicaIdType>();
                            TLogicalTimestamp logicalTimestamp = reader.Read<TLogicalTimestamp>();
                            return TSelf(timestamp, clientId, sourceReplicaId, logicalTimestamp);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<uint64_t>(m_timestamp.count())
                            .template Write<ClientIdType>(m_clientId)
                            .template Write<ReplicaIdType>(m_sourceReplicaId)
                            .template Write<TLogicalTimestamp>(m_logicalTimestamp);
                        }
                        
                    private:
                        OrbeServerToServerPutResponse(TTimestamp timestamp,
                                                      ClientIdType clientId,
                                                      ReplicaIdType sourceReplicaId,
                                                      TLogicalTimestamp logicalTimestamp)
                        : m_messageType(Wire::ServerToServerPutResponseMsg)
                        , m_timestamp(timestamp)
                        , m_clientId(clientId)
                        , m_sourceReplicaId(sourceReplicaId)
                        , m_logicalTimestamp(logicalTimestamp)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        TTimestamp m_timestamp;
                        ClientIdType m_clientId;
                        ReplicaIdType m_sourceReplicaId;
                        TLogicalTimestamp m_logicalTimestamp;
                        
                        static constexpr OrbeMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_timestamp)
                        + sizeof(m_clientId)
                        + sizeof(m_sourceReplicaId)
                        + sizeof(m_logicalTimestamp);
                    };
                }
            }
        }
    }
}
