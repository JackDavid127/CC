//
//  OrbeDelayedInstallPutReply.h
//  SimRunner
//
//  Created by Scott on 08/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include "Timing.h"
#include "OrbeDelayedInstall.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            namespace Messaging
            {
                template<typename ProtocolTraits>
                class OrbeDelayedInstallPutReplyBase
                {
                protected:
                    typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                    typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                    
                    OrbeDelayedInstallPutReplyBase()
                    {
                        
                    }
                    
                public:
                    OrbeDelayedInstallPutReplyBase(const Utilities::TTimestamp& timestamp,
                                     ReplicaIdType sourceReplicaId,
                                     TLogicalTimestamp logicalTimestamp)
                    : m_timestamp(timestamp)
                    , m_sourceReplicaId(sourceReplicaId)
                    , m_logicalTimestamp(logicalTimestamp)
                    {
                        
                    }
                    
                    const Utilities::TTimestamp& Timestamp() const
                    {
                        return m_timestamp;
                    }
                    
                    ReplicaIdType SourceReplicaId() const
                    {
                        return m_sourceReplicaId;
                    }
                    
                    TLogicalTimestamp LogicalTimestamp() const
                    {
                        return m_logicalTimestamp;
                    }
                    
                private:
                    Utilities::TTimestamp m_timestamp;
                    ReplicaIdType m_sourceReplicaId;
                    TLogicalTimestamp m_logicalTimestamp;
                };
                
                template<typename ProtocolTraits>
                class OrbeDelayedInstallPutReply : public OrbeDelayedInstallPutReplyBase<ProtocolTraits>, private boost::noncopyable
                {
                    typedef OrbeDelayedInstallPutReplyBase<ProtocolTraits> TBase;
                public:
                    OrbeDelayedInstallPutReply(const Utilities::TTimestamp& timestamp,
                                 ReplicaIdType sourceReplicaId,
                                 typename TBase::TLogicalTimestamp logicalTimestamp)
                    : OrbeDelayedInstallPutReplyBase<ProtocolTraits>(timestamp, sourceReplicaId, logicalTimestamp) { }
                };
                
                template<typename ProtocolTraits>
                class OrbeDelayedInstallPutReplySimulation : public OrbeDelayedInstallPutReplyBase<ProtocolTraits>
                {
                    typedef OrbeDelayedInstallPutReplyBase<ProtocolTraits> TBase;
                public:
                    OrbeDelayedInstallPutReplySimulation() { }
                    
                    OrbeDelayedInstallPutReplySimulation(const Utilities::TTimestamp& timestamp,
                                           ReplicaIdType sourceReplicaId,
                                           typename TBase::TLogicalTimestamp logicalTimestamp)
                    : OrbeDelayedInstallPutReplyBase<ProtocolTraits>(timestamp, sourceReplicaId, logicalTimestamp) { }
                };
            }
        }
    }
}
