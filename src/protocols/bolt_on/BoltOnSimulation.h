//
//  BoltOnSimulation.h
//  SimRunner
//
//  Created by Scott on 15/12/2014.
//

#pragma once

#include <boost/noncopyable.hpp>
#include <vector>
#include <random>
#include <memory>
#include "BoltOn.h"
#include "SimulationEvent.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            template<typename TProtocolTraits>
            class BoltOnSimulation : private boost::noncopyable
            {
                typedef typename TProtocolTraits::TKeyGenerator TKeyGenerator;
                typedef typename TProtocolTraits::TEventSchedule TEventSchedule;
                typedef typename TProtocolTraits::TBoltOnClient TClient;
                typedef typename TProtocolTraits::TShimId TShimId;
                typedef typename TProtocolTraits::TLocalShimStorage TLocalShimStorage;
                typedef typename TProtocolTraits::TLocalStoreResolver TLocalStoreResolver;
                typedef typename TProtocolTraits::TShimBackEnd TShimBackEnd;
                typedef typename TProtocolTraits::TShimDeployment TShimDeployment;
                typedef typename TProtocolTraits::TBackingStorage TBackingStorage;
                typedef typename TProtocolTraits::TRandomEngine TRandomEngine;
                typedef typename TProtocolTraits::TLogger TLogger;
                typedef typename TProtocolTraits::TMetrics TMetrics;
                typedef typename TProtocolTraits::TStorageKey TStorageKey;
                typedef typename TProtocolTraits::TValueWrapperPtr TValueWrapperPtr;
                typedef typename TProtocolTraits::TValueWrapperFactory TValueWrapperFactory;
                typedef typename TProtocolTraits::TBoltOnSerializer TSerializer;
                typedef typename TProtocolTraits::TTaskRunner TTaskRunner;
                typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;

                typedef SimRunner::SimulationInfrastructure::SimulationEvent TSimEvent;
                typedef TBackingStorage TStorageComponent;
                typedef std::unique_ptr<TStorageComponent> TStoragePtr;

            public:
                BoltOnSimulation(size_t numberOfShimDeployments,
                                 size_t numberOfClientsPerDeployment,
                                 TKeyGenerator& keyGenerator,
                                 TEventSchedule& schedule,
                                 TRandomEngine& randomEngine,
                                 TMetrics& metrics,
                                 TLogger& logger,
                                 TSerializer& serializer,
                                 TTaskRunner& taskRunner,
                                 TValueWrapperFactory& valueWrapperFactory,
                                 double putToGetRatio)
                : m_keyGenerator(keyGenerator)
                , m_schedule(schedule)
                , m_randomEngine(randomEngine)
                , m_metrics(metrics)
                , m_logger(logger)
                , m_putToGetRatio(putToGetRatio)
                , m_fullCausality(false)
                , m_distribution(0.0, 1.0)
                , m_valueWrapperFactory(valueWrapperFactory)
                , m_backingStorageComponentPtr(new TStorageComponent(*new Components::KeyValueStorage<TStorageKey, TBoltOnSerializedValueTypePtr>()))
                {
                    TShimId nextShimId(0);

                    for(size_t i = 0; i < numberOfShimDeployments; ++ i)
                    {
                        ++ nextShimId;
                        TLocalShimStorage* pLocalShimStorage(new TLocalShimStorage());
                        TLocalStoreResolver* pResolver(new TLocalStoreResolver(*m_backingStorageComponentPtr,
                                                                               *pLocalShimStorage,
                                                                               taskRunner,
                                                                               serializer,
                                                                               m_valueWrapperFactory));

                        m_shimDeployments.push_back(new TShimDeployment(nextShimId,
                                                                        *pResolver,
                                                                        *(new TShimBackEnd(nextShimId,
                                                                                           *pLocalShimStorage,
                                                                                           *pResolver,
                                                                                           serializer,
                                                                                           m_valueWrapperFactory))));

                        ClientIdType nextClientId(0);
                        for(size_t j = 0; j < numberOfClientsPerDeployment; ++ j)
                        {
                            m_clients.push_back(new TClient(++ nextClientId + (100 * nextShimId),
                                                            *m_shimDeployments.back(),
                                                            *this,
                                                            *m_backingStorageComponentPtr,
                                                            m_metrics,
                                                            false));
                        }
                    }

                    ScheduleAsyncResolverDrain(100, -1);
                }

                void PerformOperation()
                {
                    //static int m_operationCount = 0;
                    //m_logger.Log("PerformOperation %d\n", ++ m_operationCount);

                    for(auto client : m_clients)
                    {
                        if(!client->IsBusy())
                        {
                            GenerateEvent(*client);
                        }
                    }
                }

                void End()
                {

                }

                void HandleGetCompleteItemFound(const TValueWrapperPtr getReply)
                {
                    m_logger.Log("BoltOnSimulation::HandleGetCompleteItemFound: %d, %d\n",
                     getReply->Key(),
                     getReply->Value());
                }

                void HandleGetCompleteItemNotFound()
                {
                    //m_logger.Log("ECSimulation::HandleGetCompleteItemNotFound\n");
                }

                void HandlePutComplete(const TValueWrapperPtr putReply)
                {
                    m_logger.Log("BoltOnSimulation::HandlePutComplete: %d, %d\n",
                                 putReply->Key(),
                                 putReply->Value());
                }


            private:
                void GenerateEvent(TClient& client)
                {
                    TStorageKey key(m_keyGenerator.NextKey());

                    if(OperationIsRead())
                    {
                        if(OperationIsTrans()){
                            std::vector<TStorageKey> keys;
                            keys.push_back(key);
                            for (int i=1;i<2;i++){
                                TStorageKey key2(m_keyGenerator.NextKey());
                                keys.push_back(key2);
                            }
                            client.IssueGets(keys);
                        }
                        else client.IssueGet(key);
                    }
                    else
                    {
                        client.IssuePut(key, m_eventCounter);
                    }

                    ++ m_eventCounter;
                }

                void Update()
                {

                }

                bool OperationIsRead()
                {
                    double random = m_distribution(m_randomEngine);
                    return random >= m_putToGetRatio;
                }

                bool OperationIsTrans()
                {
                    //return false;
                    double random = m_distribution(m_randomEngine);
                    return random >= m_putToGetRatio;
                }

                void ScheduleAsyncResolverDrain(size_t millisecondsDelay,
                                                size_t eventCounterLastHeartbeat)
                {
                    bool noMoreEvents = eventCounterLastHeartbeat == m_eventCounter;
                    eventCounterLastHeartbeat = m_eventCounter;

                    for(auto it = m_shimDeployments.begin(); it != m_shimDeployments.end(); ++ it)
                    {
                        TShimDeployment& shimDeployment(**it);
                        shimDeployment.UpdateResolver();
                    }

                    if(!m_schedule.HasEvents() && noMoreEvents)
                    {
                        return;
                    }

                    boost::function<void ()> binding = boost::bind(&BoltOnSimulation::ScheduleAsyncResolverDrain,
                                                                   this,
                                                                   millisecondsDelay,
                                                                   eventCounterLastHeartbeat);

                    TSimEvent event = m_schedule.BuildSimulationEventTimeStampedNow(binding, "ScheduleAsyncResolverDrain");
                    m_schedule.InsertSimulationEvent(event, millisecondsDelay * 1000);
                }


            private:
                TKeyGenerator& m_keyGenerator;
                TEventSchedule& m_schedule;
                TRandomEngine& m_randomEngine;
                TMetrics& m_metrics;
                TLogger& m_logger;
                TValueWrapperFactory& m_valueWrapperFactory;
                double m_putToGetRatio;
                bool m_fullCausality;

                std::uniform_real_distribution<double> m_distribution;
                size_t m_eventCounter;
                size_t m_shimToUpdateIndex;
                std::vector<TShimDeployment*> m_shimDeployments;
                std::vector<TClient*> m_clients;
                TStoragePtr m_backingStorageComponentPtr;
            };
        }
    }
}
