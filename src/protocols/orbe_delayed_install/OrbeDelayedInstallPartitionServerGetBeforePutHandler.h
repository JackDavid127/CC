//
//  OrbeDelayedInstallPartitionServerGetBeforePutHandler.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
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
            template<typename TProtocolTraits>
            class OrbeDelayedInstallPartitionServerGetBeforePutHandler : private boost::noncopyable
            {
                typedef OrbeDelayedInstallPartitionServerGetBeforePutHandler<TProtocolTraits> TSelf;
                
                typedef typename TProtocolTraits::TSerializedDataType TSerializedDataType;
                
            public:
                OrbeDelayedInstallPartitionServerGetBeforePutHandler(OrbeDelayedInstallPartitionServer<TProtocolTraits>& partitionServer)
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
                OrbeDelayedInstallPartitionServer<TProtocolTraits>& m_partitionServer;
            };
        }
    }
}
