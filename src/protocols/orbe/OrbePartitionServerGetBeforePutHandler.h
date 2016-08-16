//
//  OrbePartitionServerGetBeforePutHandler.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
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
            template<typename TProtocolTraits>
            class OrbePartitionServerGetBeforePutHandler : private boost::noncopyable
            {
                typedef OrbePartitionServerGetBeforePutHandler<TProtocolTraits> TSelf;
                
                typedef typename TProtocolTraits::TSerializedDataType TSerializedDataType;
                
            public:
                OrbePartitionServerGetBeforePutHandler(OrbePartitionServer<TProtocolTraits>& partitionServer)
                : m_partitionServer(partitionServer)
                {
                    
                }
                
                template<typename TCompletionHandler, typename TGetBeforePutContext>
                void HandleGetCompleteItemFound(TCompletionHandler& completedHandler,
                                                const TSerializedDataType& serializedValue,
                                                const TGetBeforePutContext& getBeforePutContext)
                {
                    m_partitionServer.HandlePrePutGetCompletionItemFound(completedHandler,
                                                                         serializedValue,
                                                                         getBeforePutContext);
                }
                
                template<typename TCompletionHandler, typename TGetBeforePutContext>
                void HandleGetCompleteItemNotFound(TCompletionHandler& completedHandler,
                                                   const TGetBeforePutContext& getBeforePutContext)
                {
                    m_partitionServer.HandlePrePutGetCompletionItemNotFound(completedHandler,
                                                                            getBeforePutContext);
                }
                
            private:
                OrbePartitionServer<TProtocolTraits>& m_partitionServer;
            };
        }
    }
}
