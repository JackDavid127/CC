//
//  OrbeDelayedInstallGetReply.h
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
                class OrbeDelayedInstallGetReplyBase
                {
                protected:
                    typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                    typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                    
                    OrbeDelayedInstallGetReplyBase()
                    {
                        
                    }
                    
                public:
                    OrbeDelayedInstallGetReplyBase(const TClientInputValueType& value,
                                     const Utilities::TTimestamp& timestamp,
                                     ReplicaIdType sourceReplicaId,
                                     TLogicalTimestamp logicalTimestamp)
                    : m_value(value)
                    , m_timestamp(timestamp)
                    , m_sourceReplicaId(sourceReplicaId)
                    , m_logicalTimestamp(logicalTimestamp)
                    {
                        
                    }
                    
                    const TClientInputValueType& Value() const
                    {
                        return m_value;
                    }
                    
                    Utilities::TTimestamp Timestamp() const
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
                    TClientInputValueType m_value;
                    Utilities::TTimestamp m_timestamp;
                    ReplicaIdType m_sourceReplicaId;
                    TLogicalTimestamp m_logicalTimestamp;
                };
                
                template<typename ProtocolTraits>
                class OrbeDelayedInstallGetReply : public OrbeDelayedInstallGetReplyBase<ProtocolTraits>, private boost::noncopyable
                {
                    typedef OrbeDelayedInstallGetReplyBase<ProtocolTraits> TBase;
                public:
                    OrbeDelayedInstallGetReply(const typename TBase::TClientInputValueType& value,
                                 const Utilities::TTimestamp& timestamp,
                                 ReplicaIdType sourceReplicaId,
                                 typename TBase::TLogicalTimestamp logicalTimestamp)
                    : OrbeDelayedInstallGetReplyBase<ProtocolTraits>(value, timestamp, sourceReplicaId, logicalTimestamp) { }
                };
                
                template<typename ProtocolTraits>
                class OrbeDelayedInstallGetReplySimulation : public OrbeDelayedInstallGetReplyBase<ProtocolTraits>
                {
                    typedef OrbeDelayedInstallGetReplyBase<ProtocolTraits> TBase;
                public:
                    OrbeDelayedInstallGetReplySimulation() { }
                    
                    OrbeDelayedInstallGetReplySimulation(const typename TBase::TClientInputValueType& value,
                                           const Utilities::TTimestamp& timestamp,
                                           ReplicaIdType sourceReplicaId,
                                           typename TBase::TLogicalTimestamp logicalTimestamp)
                    : OrbeDelayedInstallGetReplyBase<ProtocolTraits>(value, timestamp, sourceReplicaId, logicalTimestamp) { }
                };
            }
        }
    }
}
