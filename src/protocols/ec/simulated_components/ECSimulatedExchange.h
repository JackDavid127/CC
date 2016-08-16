//
//  ECSimulation.h
//  SimRunner
//
//  Created by Scott on 05/10/2014.
//
//

#pragma once

#include <memory>
#include <boost/noncopyable.hpp>
#include "EC.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            namespace SimulatedComponents
            {
                namespace
                {
                    template<typename TProxiedNetworkCompleteHandlerObject>
                    class NetworkCompleteHandlerProxy
                    {
                    public:
                        NetworkCompleteHandlerProxy(TProxiedNetworkCompleteHandlerObject& proxy)
                        : m_pProxy(&proxy) { }
                        
                        template<typename TRequestMetaData>
                        void HandleLinkOperationComplete(TRequestMetaData& state)
                        {
                            m_pProxy->HandleLinkOperationComplete(state);
                        }
                        
                    private:
                        TProxiedNetworkCompleteHandlerObject* m_pProxy;
                    };
                    
                    template<typename TProxiedRequestCompleteHandlerObject>
                    class NetworkOperationStateRequestCompleteHandlerProxy
                    {
                    public:
                        NetworkOperationStateRequestCompleteHandlerProxy(TProxiedRequestCompleteHandlerObject& proxy)
                        : m_pProxy(&proxy) { }
                        
                        void Next()
                        {
                            m_pProxy->Next();
                        }
                        
                    private:
                        TProxiedRequestCompleteHandlerObject* m_pProxy;
                    };
                    
                    template<typename TProxiedRequestCompleteHandlerObject>
                    class ReplicateMessageBundleRequestCompleteHandlerProxy
                    {
                    public:
                        ReplicateMessageBundleRequestCompleteHandlerProxy(TProxiedRequestCompleteHandlerObject& proxy)
                        : m_pProxy(&proxy) { }
                        
                        void Finish()
                        {
                            m_pProxy->Finish();
                        }
                        
                    private:
                        TProxiedRequestCompleteHandlerObject* m_pProxy;
                    };

                    template <typename TPartitionServer, typename TNetworkLink>
                    class PartitionServerToLinkMapping : private boost::noncopyable
                    {
                    public:
                        PartitionServerToLinkMapping()
                        : m_pPartitionServer(nullptr)
                        , m_pNetworkLink(nullptr)
                        {
                            
                        }
                        
                        bool IsValid() const
                        {
                            return m_pPartitionServer != nullptr;
                        }
                        
                        template <typename TSchedule>
                        void Initialise(TPartitionServer& partitionServer,
                                        TSchedule& schedule)
                        {
                            SR_ASSERT(!IsValid());
                            
                            m_pPartitionServer = &partitionServer;
                            m_pNetworkLink = std::unique_ptr<TNetworkLink>(new TNetworkLink("", schedule));
                        }
                        
                        TPartitionServer& PartitionServer() const { SR_ASSERT(IsValid()); return *m_pPartitionServer; }
                        
                        TNetworkLink& Link() const { SR_ASSERT(IsValid()); return *m_pNetworkLink; }
                        
                    private:
                        TPartitionServer* m_pPartitionServer;
                        std::unique_ptr<TNetworkLink> m_pNetworkLink;
                    };
                }
                
                template <
                typename TSchedule,
                typename TMetrics,
                typename TLogger,
                typename TKey,
                typename TSerializedValueType,
                typename TDeserializedValueType,
                typename TClientInputValueType,
                typename TClientGetReplyValueType,
                typename TClientPutReplyValueType,
                typename TReplicationMessageType,
                typename TProtocolTraits>
                class ECSimulatedExchange : private boost::noncopyable
                {
                    typedef typename TProtocolTraits::TPartitionServer TPartitionServer;
                    typedef typename TProtocolTraits::TNetworkLink TNetworkLink;
                    typedef typename TProtocolTraits::TClient TClient;
                    
                    typedef ECSimulatedExchange<
                    TSchedule,
                    TMetrics,
                    TLogger,
                    TKey,
                    TSerializedValueType,
                    TDeserializedValueType,
                    TClientInputValueType,
                    TClientGetReplyValueType,
                    TClientPutReplyValueType,
                    TReplicationMessageType,
                    TProtocolTraits> TContext;
                    
                    class ReplicateMessageBundle : private boost::noncopyable
                    {
                        typedef ReplicateMessageBundle TSelf;
                        
                        TPartitionServer& m_partitionServer;
                        TReplicationMessageType m_replicateMessage;
                        TContext& m_context;
                        TNetworkLink& m_senderLink;
                        
                        ReplicateMessageBundle(TPartitionServer& partitionServer,
                                               TReplicationMessageType replicateMessage,
                                               TContext& context,
                                               TNetworkLink& senderLink)
                        : m_partitionServer(partitionServer)
                        , m_replicateMessage(replicateMessage)
                        , m_context(context)
                        , m_senderLink(senderLink)
                        {
                            m_senderLink.SendExternalNetworkMessage(NetworkCompleteHandlerProxy<TContext>(m_context),
                                                                    ReplicateMessageBundleRequestCompleteHandlerProxy<TSelf>(*this));
                        }
                        
                    public:
                        static void Dispatch(TPartitionServer& partitionServer,
                                             TReplicationMessageType replicateMessage,
                                             TContext& context,
                                             TNetworkLink& senderLink)
                        {
                            new ReplicateMessageBundle(partitionServer, replicateMessage, context, senderLink);
                        }
                        
                        void Finish()
                        {
                            m_partitionServer.ReceiveReplicateMessage(m_replicateMessage);
                            delete this;
                        }
                    };
                    
                public:
                    ECSimulatedExchange(const size_t partitionsCount,
                                        const size_t replicasCount,
                                        TSchedule& schedule,
                                        TMetrics& metrics,
                                        TLogger& logger)
                    : m_schedule(schedule)
                    , m_metrics(metrics)
                    , m_logger(logger)
                    , m_partitionServers(std::unique_ptr<PartitionServerToLinkMapping<TPartitionServer, TNetworkLink>[]>(new PartitionServerToLinkMapping<TPartitionServer, TNetworkLink>[partitionsCount * replicasCount]))
                    , m_partitionsCount(partitionsCount)
                    , m_replicasCount(replicasCount)
                    {
                        
                    }
                    
                    void AddPartitionServer(TPartitionServer& partitionServer)
                    {
                        const size_t index = GetPartitionServerIndex(partitionServer.ReplicaId(), partitionServer.PartitionId());
                        PartitionServerToLinkMapping<TPartitionServer, TNetworkLink>& mapping = m_partitionServers[index];
                        SR_ASSERT(!mapping.IsValid());
                        mapping.Initialise(partitionServer, m_schedule);
                    }
                    
                    void SendReplicationMessageToReplica(const ReplicaIdType& senderReplicaId,
                                                         const ReplicaIdType& receiverReplicaId,
                                                         const PartitionIdType& mutualPartitionId,
                                                         const TReplicationMessageType& replicateMessage)
                    {
                        TPartitionServer* pSenderPartitionServer = nullptr;
                        TNetworkLink* pSenderLink = nullptr;
                        GetPartitionServerLinkPair(senderReplicaId, mutualPartitionId, pSenderPartitionServer, pSenderLink);
                        
                        TPartitionServer* pReceiverPartitionServer = nullptr;
                        TNetworkLink* pReceiverLink = nullptr;
                        GetPartitionServerLinkPair(receiverReplicaId, mutualPartitionId, pReceiverPartitionServer, pReceiverLink);
                        
                        SR_ASSERT(pSenderPartitionServer->ReplicaId() != pReceiverPartitionServer->ReplicaId());
                        SR_ASSERT(pSenderPartitionServer->PartitionId() == pReceiverPartitionServer->PartitionId());
                        
                        ReplicateMessageBundle::Dispatch(*pReceiverPartitionServer,
                                                         replicateMessage,
                                                         *this,
                                                         *pSenderLink);
                    }
                    
                    void PerformGetAtLocalPartition(ClientIdType clientId,
                                                    TClient& client,
                                                    const TKey& key,
                                                    const ReplicaIdType& mutualReplicaId,
                                                    const PartitionIdType& requestDestinationPartitionId)
                    {
                        TPartitionServer* pPartitionServer = nullptr;
                        TNetworkLink* pLink = nullptr;
                        GetPartitionServerLinkPair(mutualReplicaId, requestDestinationPartitionId, pPartitionServer, pLink);
                        typedef NetworkOperationState<TClient> TOp;
                        TOp::DispatchGet(key, *pPartitionServer, *pLink, *this, client, m_logger);
                    }
                    
                    void PerformPutAtLocalPartition(ClientIdType clientId,
                                                    TClient& client,
                                                    const TKey& key,
                                                    const TClientInputValueType& value,
                                                    const ReplicaIdType& mutualReplicaId,
                                                    const PartitionIdType& requestDestinationPartitionId)
                    {
                        TPartitionServer* pPartitionServer = nullptr;
                        TNetworkLink* pLink = nullptr;
                        GetPartitionServerLinkPair(mutualReplicaId, requestDestinationPartitionId, pPartitionServer, pLink);
                        typedef NetworkOperationState<TClient> TOp;
                        TOp::DispatchPut(key, value, *pPartitionServer, *pLink, *this, client, m_logger);
                    }
                    
                    template <typename TRequestMetaData>
                    void HandleLinkOperationComplete(TRequestMetaData& operationState)
                    {
                        operationState.Next();
                    }
                    
                    void HandleLinkOperationComplete(ReplicateMessageBundleRequestCompleteHandlerProxy<ReplicateMessageBundle>& replicationMessageBundle)
                    {
                        replicationMessageBundle.Finish();
                    }
                    
                private:
                    size_t GetPartitionServerIndex(const ReplicaIdType& replicaId,
                                                   const PartitionIdType& partitionId) const
                    {
                        return (replicaId * m_partitionsCount) + partitionId;
                    }
                    
                    void GetPartitionServerLinkPair(const ReplicaIdType& replicaId,
                                                    const PartitionIdType& partitionId,
                                                    TPartitionServer*& out_pPartitionServer,
                                                    TNetworkLink*& out_pLink) const
                    {
                        const size_t index = GetPartitionServerIndex(replicaId, partitionId);
                        PartitionServerToLinkMapping<TPartitionServer, TNetworkLink>& mapping = m_partitionServers[index];
                        SR_ASSERT(mapping.IsValid());
                        out_pPartitionServer = &(mapping.PartitionServer());
                        out_pLink = &(mapping.Link());
                    }
                    
                    TSchedule& m_schedule;
                    TMetrics& m_metrics;
                    TLogger& m_logger;
                    std::unique_ptr<PartitionServerToLinkMapping<TPartitionServer, TNetworkLink>[]> m_partitionServers;
                    const size_t m_partitionsCount;
                    const size_t m_replicasCount;
                    
                    template<typename TCompletionHandler>
                    class NetworkOperationState : private boost::noncopyable
                    {
                        typedef NetworkOperationState<TCompletionHandler> TSelf;
                        
                    public:
                        static void DispatchGet(const TKey& key,
                                                TPartitionServer& partitionServer,
                                                TNetworkLink& link,
                                                TContext& context,
                                                TCompletionHandler& getCompletedHandler,
                                                TLogger& logger)
                        {
                            (new NetworkOperationState(key, TClientInputValueType(), partitionServer, link, context, getCompletedHandler, logger, true))->Next();
                        }
                        
                        static void DispatchPut(const TKey& key,
                                                const TClientInputValueType& value,
                                                TPartitionServer& partitionServer,
                                                TNetworkLink& link,
                                                TContext& context,
                                                TCompletionHandler& putCompletedHandler,
                                                TLogger& logger)
                        {
                            (new NetworkOperationState(key, value, partitionServer, link, context, putCompletedHandler, logger, false))->Next();
                        }
                        
                        void Next()
                        {
                            size_t currentPhase = m_phase++;
                            
                            switch(currentPhase)
                            {
                                case 0:
                                {
                                    // send the message to the other partition server
                                    m_link.SendInternalNetworkMessage(NetworkCompleteHandlerProxy<TContext>(m_context),
                                                                      NetworkOperationStateRequestCompleteHandlerProxy<TSelf>(*this));
                                }break;
                                    
                                case 1:
                                {
                                    // do the operation at the local partition server
                                    if(m_isGet)
                                    {
                                        m_partitionServer.Get(m_key, *this);
                                    }
                                    else
                                    {
                                        m_partitionServer.Put(m_key, m_putValue, *this);
                                    }
                                }break;
                                    
                                case 2:
                                {
                                    // finally, send the get response back to the requesting partition
                                    m_link.SendInternalNetworkMessage(NetworkCompleteHandlerProxy<TContext>(m_context),
                                                                      NetworkOperationStateRequestCompleteHandlerProxy<TSelf>(*this));
                                }break;
                                    
                                case 3:
                                {
                                    if(m_isGet)
                                    {
                                        SR_ASSERT(!m_gotPutValue);
                                        
                                        if(m_gotGetValue)
                                        {
                                            m_completionHandler.HandleGetCompleteItemFound(m_valueFromGet);
                                        }
                                        else
                                        {
                                            m_completionHandler.HandleGetCompleteItemNotFound();
                                        }
                                    }
                                    else
                                    {
                                        SR_ASSERT(!m_gotGetValue);
                                        SR_ASSERT(m_gotPutValue);
                                        
                                        m_completionHandler.HandlePutComplete(m_valueFromPut);
                                    }
                                    
                                    delete this;
                                }break;
                                    
                                default:
                                {
                                    SR_ASSERT(false);
                                }break;
                            }
                        }
                        
                        void HandleGetCompleteItemFound(const TClientGetReplyValueType& value)
                        {
                            SR_ASSERT(m_isGet);
                            m_valueFromGet = value;
                            m_gotGetValue = true;
                            Next();
                        }
                        
                        void HandleGetCompleteItemNotFound()
                        {
                            SR_ASSERT(m_isGet);
                            m_gotGetValue = false;
                            Next();
                        }
                        
                        void HandlePutComplete(const TClientPutReplyValueType& value)
                        {
                            SR_ASSERT(!m_isGet);
                            m_valueFromPut = value;
                            m_gotPutValue = true;
                            Next();
                        }
                        
                    private:
                        NetworkOperationState(const TKey& key,
                                              const TClientInputValueType& valueToPut,
                                              TPartitionServer& partitionServer,
                                              TNetworkLink& link,
                                              TContext& context,
                                              TCompletionHandler& completionHandler,
                                              TLogger& logger,
                                              bool isGet)
                        : m_key(key)
                        , m_putValue(valueToPut)
                        , m_partitionServer(partitionServer)
                        , m_link(link)
                        , m_context(context)
                        , m_completionHandler(completionHandler)
                        , m_logger(logger)
                        , m_phase(0)
                        , m_gotGetValue(false)
                        , m_gotPutValue(false)
                        , m_isGet(isGet)
                        {
                            
                        }
                        
                        TKey m_key;
                        TPartitionServer& m_partitionServer;
                        TNetworkLink& m_link;
                        TContext& m_context;
                        TCompletionHandler& m_completionHandler;
                        TLogger& m_logger;
                        size_t m_phase;
                        TClientInputValueType m_putValue;
                        TClientGetReplyValueType m_valueFromGet;
                        TClientPutReplyValueType m_valueFromPut;
                        bool m_gotGetValue;
                        bool m_gotPutValue;
                        bool m_isGet;
                    };
                };
            }
        }
    }
}
