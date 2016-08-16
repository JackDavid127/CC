//
//  OrbeDelayedInstallDependencyTimestampCheckRequest.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "OrbeDelayedInstall.h"
#include "OrbeDelayedInstallWire.h"

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
                    class OrbeDelayedInstallDependencyTimestampCheckRequest
                    {
                        typedef OrbeDelayedInstallDependencyTimestampCheckRequest TSelf;
                        typedef typename ProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                        
                    public:
                        static TSelf CreateLocal(PartitionIdType senderPartitionId,
                                                 ReplicaIdType sourceReplicaId,
                                                 const TClientDependencyTimestamp& clientDependencyTimestamp)
                        {
                            return TSelf(false, senderPartitionId, sourceReplicaId, clientDependencyTimestamp);
                        }
                        
                        static TSelf CreateGlobal(PartitionIdType senderPartitionId,
                                                  ReplicaIdType sourceReplicaId,
                                                  const TClientDependencyTimestamp& clientDependencyTimestamp)
                        {
                            return TSelf(true, senderPartitionId, sourceReplicaId, clientDependencyTimestamp);
                        }
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }
                        
                        bool IsGlobalVectorCheck() const { return m_isGlobalVectorCheck; }
                        
                        PartitionIdType SenderPartitionId() const { return m_senderPartitionId; }
                        
                        ReplicaIdType SourceReplicaId() const { return m_sourceReplicaId; }
                        
                        const TClientDependencyTimestamp& ClientDependencyTimestamp() const { return m_clientDependencyTimestamp; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            TBufferReader reader(buffer);
                            
                            bool isGlobalCheck = reader.Read<bool>();
                            PartitionIdType senderPartitionId = reader.Read<PartitionIdType>();
                            ReplicaIdType sourceReplicaId = reader.Read<ReplicaIdType>();
                            TClientDependencyTimestamp clientDependencyTimestamp = reader.Read<TClientDependencyTimestamp>();
                            
                            return TSelf(isGlobalCheck,
                                         senderPartitionId,
                                         sourceReplicaId,
                                         clientDependencyTimestamp);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType> TBufferWriter;
                            TBufferWriter writer(buffer, BodySize);
                            
                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<bool>(m_isGlobalVectorCheck)
                            .template Write<PartitionIdType>(m_senderPartitionId)
                            .template Write<ReplicaIdType>(m_sourceReplicaId)
                            .template Write<TClientDependencyTimestamp>(m_clientDependencyTimestamp);
                        }
                        
                    private:
                        OrbeDelayedInstallDependencyTimestampCheckRequest(bool isGlobalCheck,
                                                                          PartitionIdType senderPartitionId,
                                                                          ReplicaIdType sourceReplicaId,
                                                                          const TClientDependencyTimestamp& clientDependencyTimestamp)
                        : m_messageType(Wire::DependencyTimestampCheckRequestMsg)
                        , m_isGlobalVectorCheck(isGlobalCheck)
                        , m_senderPartitionId(senderPartitionId)
                        , m_sourceReplicaId(sourceReplicaId)
                        , m_clientDependencyTimestamp(clientDependencyTimestamp)
                        {
                            
                        }
                        
                        OrbeDelayedInstallWireMessages m_messageType;
                        bool m_isGlobalVectorCheck;
                        PartitionIdType m_senderPartitionId;
                        ReplicaIdType m_sourceReplicaId;
                        TClientDependencyTimestamp m_clientDependencyTimestamp;
                        
                        static constexpr OrbeDelayedInstallMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_isGlobalVectorCheck)
                        + sizeof(m_senderPartitionId)
                        + sizeof(m_sourceReplicaId)
                        + sizeof(m_clientDependencyTimestamp);
                    };
                }
            }
        }
    }
}
