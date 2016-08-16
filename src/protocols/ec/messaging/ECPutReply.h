//
//  ECPutReply.h
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
                class ECPutReplyBase
                {
                protected:
                    ECPutReplyBase()
                    {
                        
                    }
                    
                public:
                    ECPutReplyBase(const Utilities::TTimestamp& timestamp,
                                   ReplicaIdType sourceReplicaId)
                    : m_timestamp(timestamp)
                    , m_sourceReplicaId(sourceReplicaId)
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
                    
                private:
                    Utilities::TTimestamp m_timestamp;
                    ReplicaIdType m_sourceReplicaId;
                };
                
                class ECPutReply : public ECPutReplyBase, private boost::noncopyable
                {
                public:
                    ECPutReply(const Utilities::TTimestamp& timestamp,
                               ReplicaIdType sourceReplicaId)
                    : ECPutReplyBase(timestamp, sourceReplicaId) { }
                };
                
                class ECPutReplySimulation : public ECPutReplyBase
                {
                public:
                    ECPutReplySimulation() { }
                    
                    ECPutReplySimulation(const Utilities::TTimestamp& timestamp,
                                         ReplicaIdType sourceReplicaId)
                    : ECPutReplyBase(timestamp, sourceReplicaId) { }
                };
            }
        }
    }
}
