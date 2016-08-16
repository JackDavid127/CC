//
//  ECPartitionServer.h
//  SimRunner
//
//  Created by Scott on 06/10/2014.
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
            template<
            typename TKey,
            typename TSerializedValueType,
            typename TDeserializedValueType,
            typename TClientInputValueType,
            typename TClientGetReplyValueType,
            typename TClientPutReplyValueType,
            typename TReplicationMessageType,
            typename TStorage,
            typename TMetrics,
            typename TDelayTracking,
            typename TLogger,
            typename TProtocolTraits
            >
            class ECPartitionServer : private boost::noncopyable
            {
                typedef void* NoContext;
                static constexpr void* EmptyContext = nullptr;
                
                typedef ECPartitionServer<
                TKey,
                TSerializedValueType,
                TDeserializedValueType,
                TClientInputValueType,
                TClientGetReplyValueType,
                TClientPutReplyValueType,
                TReplicationMessageType,
                TStorage,
                TMetrics,
                TDelayTracking,
                TLogger,
                TProtocolTraits> TSelf;
                
                typedef typename TProtocolTraits::TNetworkExchange TNetworkExchange;
                typedef typename TProtocolTraits::TBroadcaster TBroadcaster;
                typedef typename TProtocolTraits::TSerializer TSerializer;
                
            public:
                ECPartitionServer(ReplicaIdType replicaId,
                                  PartitionIdType partitionId,
                                  TStorage& storage,
                                  TNetworkExchange& networkExchange,
                                  TBroadcaster& replicaPartitionsBroadcaster,
                                  TSerializer& serializer,
                                  TMetrics& metrics,
                                  TDelayTracking& delayTracking,
                                  TLogger& logger)
                : m_replicaId(replicaId)
                , m_partitionId(partitionId)
                , m_storage(storage)
                , m_networkExchange(networkExchange)
                , m_replicaPartitionsBroadcaster(replicaPartitionsBroadcaster)
                , m_serializer(serializer)
                , m_metrics(metrics)
                , m_delayTracking(delayTracking)
                , m_logger(logger)
                , m_replicationHandler(logger, storage, serializer)
                {
                    
                }
                
                ~ECPartitionServer()
                {
                    
                }
                
                ReplicaIdType ReplicaId() const
                {
                    return m_replicaId;
                }
                
                PartitionIdType PartitionId() const
                {
                    return m_partitionId;
                }
                
                size_t TotalUnappliedReplications() const
                {
                    return 0;
                }
                
                template<typename TGetCompletedHandler, typename TContext = NoContext>
                void Get(const TKey& key,
                         TGetCompletedHandler& getCompletedHandler,
                         TContext context = EmptyContext)
                {
                    //self.__num_gets.increment_and_get();
                    m_storage.Get(key, *this, getCompletedHandler, context);
                }
            
                template<typename TPutCompletedHandler, typename TContext = NoContext>
                void Put(const TKey& key,
                         const TClientInputValueType& input,
                         TPutCompletedHandler& putCompletedHandler,
                         TContext context = EmptyContext)
                {
                    //self.__num_puts.increment_and_get()
                    
                    //m_logger.Log("partition server replica %d, partition %d\n", m_replicaId, m_partitionId);
                    
                    Utilities::TTimestamp timestamp = Utilities::Now();
                    TDeserializedValueType value(key, input, timestamp, m_replicaId);
                    TSerializedValueType serializedValue = m_serializer.Serialize(value);
                    m_storage.Put(key, serializedValue, *this, putCompletedHandler, value, context);
                }
                
                template<typename TGetCompletedHandler, typename TContext = NoContext>
                void HandleGetCompleteItemFound(TGetCompletedHandler& getCompletedHandler,
                                                const TSerializedValueType& serializedValue,
                                                TContext context)
                {
                    //m_metrics.add_partition_server_get(m_partitionId, item)
                    
                    TDeserializedValueType protocolItem = m_serializer.Deserialize(serializedValue);

                    TClientGetReplyValueType reply(protocolItem.Value(),
                                                   protocolItem.Timestamp(),
                                                   protocolItem.SourceReplicaId());
                    
                    DispatchToGetCompleteItemFoundHandler(getCompletedHandler, reply, context);
                }
                
                template<typename TGetCompletedHandler, typename TContext = NoContext>
                void HandleGetCompleteItemNotFound(TGetCompletedHandler& getCompletedHandler,
                                                   TContext context)
                {
                    getCompletedHandler.HandleGetCompleteItemNotFound(context);
                }
                
                template<typename TGetCompletedHandler>
                void HandleGetCompleteItemNotFound(TGetCompletedHandler& getCompletedHandler,
                                                   NoContext context)
                {
                    getCompletedHandler.HandleGetCompleteItemNotFound();
                }
                
                template<typename TPutCompletedHandler, typename TContext = NoContext>
                void HandlePutComplete(TPutCompletedHandler& putCompletedHandler,
                                       const TDeserializedValueType& protocolItem,
                                       TContext context)
                {
                    TReplicationMessageType message(protocolItem.Key(),
                                                    protocolItem.Value(),
                                                    protocolItem.Timestamp(),
                                                    protocolItem.SourceReplicaId(),
                                                    m_partitionId);
                
                    m_replicaPartitionsBroadcaster.BroadcastReplicationToPartitionReplicas(message);
                    
                    TClientPutReplyValueType reply(protocolItem.Timestamp(),
                                                   protocolItem.SourceReplicaId());

                    DispatchToPutCompleteHandler(putCompletedHandler, reply, context);
                }
                
                void ReceiveReplicateMessage(const TReplicationMessageType& replicateMessage)
                {
                    m_logger.Log("ECPartitionServer::ReceiveReplicateMessage: %s, %llu, %d\n",
                                 replicateMessage.Value().c_str(),
                                 replicateMessage.Timestamp(),
                                 replicateMessage.SourceReplicaId());
                    
                    //self.__num_replications.increment_and_get()
                    
                    SR_ASSERT(replicateMessage.PartitionServerId() == m_partitionId);
                    SR_ASSERT(replicateMessage.SourceReplicaId() != m_replicaId);
                    
                    //self.__metrics.add_replicate_message_received_at_data_centre_replica(self.__replica_id, replicate_message.source_replica_id)
                    
                    //self.__metrics.add_partition_server_receives_replicate_message(self.__partition_id, self.__replica_id, replicate_message.source_replica_id)
                    
                    m_replicationHandler.ReceiveReplicateMessage(replicateMessage);
                }
                
            private:
                template<typename TGetCompletedHandler, typename TContext>
                void DispatchToGetCompleteItemFoundHandler(TGetCompletedHandler& getCompletedHandler,
                                                           TClientGetReplyValueType& reply,
                                                           TContext context)
                {
                    getCompletedHandler.HandleGetCompleteItemFound(reply, context);
                }
                
                
                template<typename TGetCompletedHandler>
                void DispatchToGetCompleteItemFoundHandler(TGetCompletedHandler& getCompletedHandler,
                                                           TClientGetReplyValueType& reply,
                                                           NoContext context)
                {
                    getCompletedHandler.HandleGetCompleteItemFound(reply);
                }
                
                template<typename TPutCompletedHandler, typename TContext>
                void DispatchToPutCompleteHandler(TPutCompletedHandler& putCompletedHandler,
                                                  TClientPutReplyValueType& reply,
                                                  TContext context)
                {
                    putCompletedHandler.HandlePutComplete(reply, context);
                }
                
                
                template<typename TPutCompletedHandler>
                void DispatchToPutCompleteHandler(TPutCompletedHandler& putCompletedHandler,
                                                  TClientPutReplyValueType& reply,
                                                  NoContext context)
                {
                    putCompletedHandler.HandlePutComplete(reply);
                }
                
                
                
                ReplicaIdType m_replicaId;
                PartitionIdType m_partitionId;
                TStorage& m_storage;
                TNetworkExchange& m_networkExchange;
                TBroadcaster& m_replicaPartitionsBroadcaster;
                TSerializer& m_serializer;
                TMetrics& m_metrics;
                TDelayTracking& m_delayTracking;
                TLogger& m_logger;
                
                class ReplicationHandler : private boost::noncopyable
                {
                public:
                    ReplicationHandler(TLogger& logger,
                                       TStorage& storage,
                                       TSerializer& serializer)
                    : m_logger(logger)
                    , m_storage(storage)
                    , m_serializer(serializer)
                    {
                    }
                    
                    void ReceiveReplicateMessage(const TReplicationMessageType& replicateMessage)
                    {
                        m_storage.Get(replicateMessage.Key(), *this, replicateMessage, EmptyContext);
                    }
                    
                    template<typename TContext = NoContext>
                    void HandleGetCompleteItemFound(const TReplicationMessageType& replicateMessage,
                                                    const TSerializedValueType& serializedValue,
                                                    TContext context = EmptyContext)
                    {
                        m_logger.Log("ReplicationHandler::HandleGetCompleteItemFound: %d, %s, %llu, %d\n",
                                     replicateMessage.Key(),
                                     replicateMessage.Value().c_str(),
                                     replicateMessage.Timestamp(),
                                     replicateMessage.SourceReplicaId());
                        
                        bool useNewItem = true;
                        
                        TDeserializedValueType existingItem = m_serializer.Deserialize(serializedValue);
                        
                        if(existingItem.Timestamp() > replicateMessage.Timestamp())
                        {
                            useNewItem = false;
                        }
                        else
                        {
                            if(existingItem.Timestamp() == replicateMessage.Timestamp())
                            {
                                if(existingItem.SourceReplicaId() > replicateMessage.SourceReplicaId())
                                {
                                    useNewItem = false;
                                }
                            }
                        }
                        
                        if(useNewItem)
                        {
                            TDeserializedValueType newValue = replicateMessage.ToValue();
                            TSerializedValueType serializedNewValue = m_serializer.Serialize(newValue);
                            
                            //self.__delay_tracking.operation_performed(new_ec_item)
                            
                            m_storage.Put(newValue.Key(),
                                          serializedNewValue,
                                          *this,
                                          replicateMessage,
                                          newValue);
                        }
                    }
                    
                    template<typename TContext = NoContext>
                    void HandleGetCompleteItemNotFound(const TReplicationMessageType& replicateMessage,
                                                       TContext context = EmptyContext)
                    {
                        m_logger.Log("ReplicationHandler::HandleGetCompleteItemNotFound: %d, %s, %llu, %d\n",
                                     replicateMessage.Key(),
                                     replicateMessage.Value().c_str(),
                                     replicateMessage.Timestamp(),
                                     replicateMessage.SourceReplicaId());
                        
                        TDeserializedValueType protocolItem = replicateMessage.ToValue();
                        //self.__delay_tracking.operation_performed(new_ec_item)
                        TSerializedValueType serializedValue = m_serializer.Serialize(protocolItem);
                        
                        m_storage.Put(protocolItem.Key(),
                                      serializedValue,
                                      *this,
                                      replicateMessage,
                                      protocolItem);
                    }
                    
                    template<typename TContext = NoContext>
                    void HandlePutComplete(const TReplicationMessageType& replicateMessage,
                                           const TDeserializedValueType& protocolItem,
                                           TContext context = EmptyContext)
                    {
                        m_logger.Log("ReplicationHandler::HandlePutComplete: %d, %s, %llu, %d\n",
                                     replicateMessage.Key(),
                                     replicateMessage.Value().c_str(),
                                     replicateMessage.Timestamp(),
                                     replicateMessage.SourceReplicaId());
                    }
                private:
                    TLogger& m_logger;
                    TStorage& m_storage;
                    TSerializer& m_serializer;
                };
                
                ReplicationHandler m_replicationHandler;
            };
        }
    }
}
