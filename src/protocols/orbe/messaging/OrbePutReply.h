//
//  OrbePutReply.h
//  SimRunner
//
//  Created by Scott on 08/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include "Timing.h"
#include "Orbe.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace Orbe
        {
            namespace Messaging
            {
                template<typename ProtocolTraits>
                class OrbePutReplyBase
                {
                protected:
                    typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                    typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                    
                    OrbePutReplyBase()
                    {
                        
                    }
                    
                public:
                    OrbePutReplyBase(const Utilities::TTimestamp& timestamp,
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
                class OrbePutReply : public OrbePutReplyBase<ProtocolTraits>, private boost::noncopyable
                {
                    typedef OrbePutReplyBase<ProtocolTraits> TBase;
                public:
                    OrbePutReply(const Utilities::TTimestamp& timestamp,
                                 ReplicaIdType sourceReplicaId,
                                 typename TBase::TLogicalTimestamp logicalTimestamp)
                    : OrbePutReplyBase<ProtocolTraits>(timestamp, sourceReplicaId, logicalTimestamp) { }
                };
                
                template<typename ProtocolTraits>
                class OrbePutReplySimulation : public OrbePutReplyBase<ProtocolTraits>
                {
                    typedef OrbePutReplyBase<ProtocolTraits> TBase;
                public:
                    OrbePutReplySimulation() { }
                    
                    OrbePutReplySimulation(const Utilities::TTimestamp& timestamp,
                                           ReplicaIdType sourceReplicaId,
                                           typename TBase::TLogicalTimestamp logicalTimestamp)
                    : OrbePutReplyBase<ProtocolTraits>(timestamp, sourceReplicaId, logicalTimestamp) { }
                };
            }
        }
    }
}
