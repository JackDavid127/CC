//
//  OrbeDelayedInstallDependencyTimestampCheckResponse.h
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
                    class OrbeDelayedInstallDependencyTimestampCheckResponse
                    {
                        typedef OrbeDelayedInstallDependencyTimestampCheckResponse TSelf;
                    public:
                        static TSelf CreateLocal(ReplicaIdType sourceReplicaId)
                        {
                            return TSelf(false, sourceReplicaId);
                        }
                        
                        static TSelf CreateGlobal(ReplicaIdType sourceReplicaId)
                        {
                            return TSelf(true, sourceReplicaId);
                        }
                        
                        OrbeDelayedInstallWireMessages MessageType() const { return m_messageType; }
                        
                        bool IsGlobalVectorCheck() const { return m_isGlobalVectorCheck; }
                        
                        ReplicaIdType SourceReplicaId() const { return m_sourceReplicaId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<OrbeDelayedInstallMessagePreamble> reader(buffer);
                            
                            bool isGlobalCheck = reader.Read<bool>();
                            ReplicaIdType sourceReplicaId = reader.Read<ReplicaIdType>();
                            
                            return TSelf(isGlobalCheck, sourceReplicaId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<OrbeDelayedInstallMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<bool>(m_isGlobalVectorCheck)
                            .template Write<ReplicaIdType>(m_sourceReplicaId);
                        }
                        
                    private:
                        OrbeDelayedInstallDependencyTimestampCheckResponse(bool isGlobalCheck,
                                                                           ReplicaIdType sourceReplicaId)
                        : m_messageType(Wire::DependencyTimestampCheckResponseMsg)
                        , m_isGlobalVectorCheck(isGlobalCheck)
                        , m_sourceReplicaId(sourceReplicaId)
                        {
                            
                        }
                        
                        OrbeDelayedInstallWireMessages m_messageType;
                        bool m_isGlobalVectorCheck;
                        ReplicaIdType m_sourceReplicaId;
                        
                        static constexpr OrbeDelayedInstallMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_isGlobalVectorCheck)
                        + sizeof(m_sourceReplicaId);
                    };
                }
            }
        }
    }
}
