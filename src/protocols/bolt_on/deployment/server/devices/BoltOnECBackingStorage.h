//
//  BoltOnSimulatedBackingStorage.h
//  SimRunner
//
//  Created by Scott on 17/12/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <memory>
#include "SimRunnerAssert.h"
#include "BoltOnECTransportSerializer.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace Deployment
            {
                namespace Server
                {
                    namespace Devices
                    {
                        template<typename TProtocolTraits>
                        class BoltOnECBackingStorage : private boost::noncopyable
                        {
                            typedef typename TProtocolTraits::TStorageKey TStorageKey;
                            typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;
                            typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                            typedef typename TProtocolTraits::TNetworkExchange TNetworkExchange;
                            typedef typename TProtocolTraits::TKeyToPartitionMapper TKeyToPartitionMapper;
                            typedef typename TProtocolTraits::TValueWrapperDataFactory TValueWrapperDataFactory;
                            
                            typedef BoltOnECTransportSerializer<TProtocolTraits> TTransportSerializer;
                            
                        public:
                            BoltOnECBackingStorage(ClientIdType clientId,
                                                   TKeyToPartitionMapper& keyToPartitionMapper,
                                                   TPartitionServer& partitionServer,
                                                   TNetworkExchange& networkExchange,
                                                   TValueWrapperDataFactory& valueWrapperDataFactory)
                            : m_clientId(clientId)
                            , m_keyToPartitionMapper(keyToPartitionMapper)
                            , m_partitionServer(partitionServer)
                            , m_networkExchange(networkExchange)
                            , m_valueWrapperDataFactory(valueWrapperDataFactory)
                            , m_busy(false)
                            {
                                
                            }
                            
                            std::unique_ptr<BoltOnECBackingStorage> Clone()
                            {
                                static int cloneId = -2;
                                return std::unique_ptr<BoltOnECBackingStorage>(new BoltOnECBackingStorage(cloneId--,
                                                                                                          m_keyToPartitionMapper,
                                                                                                          m_partitionServer,
                                                                                                          m_networkExchange,
                                                                                                          m_valueWrapperDataFactory));
                            }
                            
                            void Get(const TStorageKey& key,
                                     boost::function<void (const TBoltOnSerializedValueTypePtr)> getCompletionCallback)
                            {
                                SR_ASSERT(!m_busy, "Client %d is busy!", m_clientId);
                                m_busy = true;
                                m_currentGetCompletionCallback = getCompletionCallback;
                                
                                const PartitionIdType partitionServerId = m_keyToPartitionMapper.PartitionForKey(key);
                                const bool keyResidesOnLocalPartition = m_partitionServer.PartitionId() == partitionServerId;
                                
                                if(keyResidesOnLocalPartition)
                                {
                                    m_partitionServer.Get(key, *this);
                                }
                                else
                                {
                                    m_networkExchange.PerformGetAtLocalPartition(m_clientId,
                                                                                 *this,
                                                                                 key,
                                                                                 m_partitionServer.ReplicaId(),
                                                                                 partitionServerId);
                                }
                            }
                            
                            void Put(const TStorageKey& key,
                                     const TBoltOnSerializedValueTypePtr serializedData,
                                     const boost::function<void ()>& putCompleteHandler)
                            {
                                SR_ASSERT(!m_busy, "Client %d is busy!", m_clientId);
                                
                                m_busy = true;
                                m_currentPutCompletionCallback = putCompleteHandler;
                                
                                const PartitionIdType partitionServerId = m_keyToPartitionMapper.PartitionForKey(key);
                                const bool keyResidesOnLocalPartition = m_partitionServer.PartitionId() == partitionServerId;
                                
                                std::string json;
                                m_transportSerializer.SerializeBoltOnPayloadToJsonString(serializedData, json);
                                
                                if(keyResidesOnLocalPartition)
                                {
                                    m_partitionServer.Put(key, json, *this);
                                }
                                else
                                {
                                    m_networkExchange.PerformPutAtLocalPartition(m_clientId,
                                                                                 *this,
                                                                                 key,
                                                                                 json,
                                                                                 m_partitionServer.ReplicaId(),
                                                                                 partitionServerId);
                                }
                            }
                            
                            bool IsBusy() const
                            {
                                return m_busy;
                            }
                            
                            template <typename TClientGetReplyValueType>
                            void HandleGetCompleteItemFound(const TClientGetReplyValueType& getReply)
                            {
                                SR_ASSERT(m_busy);
                                SR_ASSERT(getReply.Value().size() > 0);
                                
                                m_busy = false;
                                
                                TBoltOnSerializedValueTypePtr serializedData(m_valueWrapperDataFactory.Create());
                                m_transportSerializer.DeserializeBoltOnPayloadFromJsonString(getReply.Value(), serializedData);
                                m_currentGetCompletionCallback(serializedData);
                            }
                            
                            void HandleGetCompleteItemNotFound()
                            {
                                SR_ASSERT(m_busy);
                                m_busy = false;
                                m_currentGetCompletionCallback(nullptr);
                            }
                            
                            template <typename TClientPutReplyValueType>
                            void HandlePutComplete(const TClientPutReplyValueType& value)
                            {
                                SR_ASSERT(m_busy);
                                m_busy = false;
                                m_currentPutCompletionCallback();
                            }
                            
                        private:
                            ClientIdType m_clientId;
                            TKeyToPartitionMapper& m_keyToPartitionMapper;
                            TPartitionServer& m_partitionServer;
                            TNetworkExchange& m_networkExchange;
                            TTransportSerializer m_transportSerializer;
                            TValueWrapperDataFactory& m_valueWrapperDataFactory;
                            
                            bool m_busy;
                            boost::function<void (const TBoltOnSerializedValueTypePtr)> m_currentGetCompletionCallback;
                            boost::function<void ()> m_currentPutCompletionCallback;
                        };
                    }
                }
            }
        }
    }
}
