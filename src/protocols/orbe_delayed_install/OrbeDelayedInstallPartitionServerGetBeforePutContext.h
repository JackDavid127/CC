//
//  OrbeDelayedInstallPartitionServerGetBeforePutContext.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
//
//

#pragma once

#include <boost/optional.hpp>
#include "SimRunnerAssert.h"
#include "OrbeDelayedInstall.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace OrbeDelayedInstall
        {
            template<typename TProtocolTraits, typename TPutCompletedHandler, typename TContext>
            class OrbeDelayedInstallPartitionServerGetBeforePutContext
            {
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TClientId TClientId;
                typedef typename TProtocolTraits::TClientDependencyTimestamp TClientDependencyTimestamp;
                typedef typename TProtocolTraits::TClientDependencyPartition TClientDependencyPartition;
                typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                typedef typename TProtocolTraits::TDeserializedValueType TDeserializedValueType;
                
            public:
                OrbeDelayedInstallPartitionServerGetBeforePutContext(const TStorageKey& key,
                                                                     const TClientId& clientId,
                                                                     const TClientDependencyTimestamp& clientDependencyTimestamp,
                                                                     const TClientDependencyPartition& clientDependencyPartition,
                                                                     const TClientInputValueType& value,
                                                                     TPutCompletedHandler& putCompletedHandler,
                                                                     const TContext& context,
                                                                     boost::optional<TDeserializedValueType> clientUpdateSpaceValue)
                : m_key(key)
                , m_clientId(clientId)
                , m_clientDependencyTimestamp(clientDependencyTimestamp)
                , m_clientDependencyPartition(clientDependencyPartition)
                , m_value(value)
                , m_putCompletedHandler(putCompletedHandler)
                , m_context(context)
                , m_clientUpdateSpaceValue(clientUpdateSpaceValue)
                {
                    
                }
                
                const TStorageKey& Key() const { return m_key; }
                
                const TClientId ClientId() const { return m_clientId; }
                
                const TClientDependencyTimestamp ClientDependencyTimestamp() const { return m_clientDependencyTimestamp; }
                
                const TClientDependencyPartition ClientDependencyPartition() const { return m_clientDependencyPartition; }
                
                const TClientInputValueType& Value() const { return m_value; }
                
                TPutCompletedHandler& PutCompletedHandler() const { return m_putCompletedHandler; }
                
                const TContext& Context() const { return m_context; }
                
                const bool HasClientUpdateSpaceValue() const
                {
                    return !!m_clientUpdateSpaceValue;
                }
                
                const TDeserializedValueType& ClientUpdateSpaceValue() const
                {
                    SR_ASSERT(HasClientUpdateSpaceValue());
                    return *m_clientUpdateSpaceValue;
                }
                
            private:
                TStorageKey m_key;
                TClientId m_clientId;
                TClientDependencyTimestamp m_clientDependencyTimestamp;
                TClientDependencyPartition m_clientDependencyPartition;
                TClientInputValueType m_value;
                TPutCompletedHandler& m_putCompletedHandler;
                TContext m_context;
                boost::optional<TDeserializedValueType> m_clientUpdateSpaceValue;
            };
        }
    }
}
