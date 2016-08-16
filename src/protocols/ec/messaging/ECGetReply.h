//
//  ECGetReply.h
//  SimRunner
//
//  Created by Scott on 08/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include "Timing.h"
#include "EC.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            namespace Messaging
            {
                template<typename TClientInputValueType>
                class ECGetReplyBase
                {
                protected:
                    ECGetReplyBase()
                    {
                        
                    }
                    
                public:
                    ECGetReplyBase(const TClientInputValueType& value,
                                   const Utilities::TTimestamp& timestamp,
                                   ReplicaIdType sourceReplicaId)
                    : m_value(value)
                    , m_timestamp(timestamp)
                    , m_sourceReplicaId(sourceReplicaId)
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
                    
                private:
                    TClientInputValueType m_value;
                    Utilities::TTimestamp m_timestamp;
                    ReplicaIdType m_sourceReplicaId;
                };
                
                template<typename TClientInputValueType>
                class ECGetReply : public ECGetReplyBase<TClientInputValueType>, private boost::noncopyable
                {
                public:
                    ECGetReply(const TClientInputValueType& value,
                               const Utilities::TTimestamp& timestamp,
                               ReplicaIdType sourceReplicaId)
                    : ECGetReplyBase<TClientInputValueType>(value, timestamp, sourceReplicaId) { }
                };
                
                template<typename TClientInputValueType>
                class ECGetReplySimulation : public ECGetReplyBase<TClientInputValueType>
                {
                public:
                    ECGetReplySimulation() { }
                    
                    ECGetReplySimulation(const TClientInputValueType& value,
                                         const Utilities::TTimestamp& timestamp,
                                         ReplicaIdType sourceReplicaId)
                    : ECGetReplyBase<TClientInputValueType>(value, timestamp, sourceReplicaId) { }
                };
            }
        }
    }
}
