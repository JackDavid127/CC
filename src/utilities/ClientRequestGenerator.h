//
//  ClientRequestGenerator.h
//  SimRunner
//
//  Created by Scott on 26/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <string>
#include <cstdlib>
#include <iostream>
#include "Timing.h"

namespace SimRunner
{
    namespace Utilities
    {
        template<typename ProtocolTraits>
        class ClientRequestGenerator
        {
            typedef typename ProtocolTraits::TKeyType TKeyType;
            typedef typename ProtocolTraits::TRandomEngine TRandomEngine;
            typedef typename ProtocolTraits::TKeyDistribution TKeyDistribution;
            
        public:
            ClientRequestGenerator(TKeyDistribution& keyDistribution)
            : m_keyDistribution(keyDistribution)
            , m_requestsDispatched(0)
            , m_lastPrint(Utilities::Now())
            , m_startTime(Utilities::Now())
            {
                
            }
            
            TKeyType NextKey()
            {
                Utilities::TTimestamp now = Utilities::Now();
                
                if(Utilities::GetDeltaTime<Seconds>(now, m_lastPrint).count() > 10)
                {
                    m_lastPrint = now;
                    double runTime(Utilities::GetDeltaTime<Utilities::Seconds>(now, m_startTime).count());
                    uint32_t requestsPerSecond = static_cast<uint32_t>(m_requestsDispatched / runTime);
                    printf("%d RPS\n", requestsPerSecond);
                }
                
                m_requestsDispatched += 1;
                return m_keyDistribution.NextKey();
            }
            
        private:
            TKeyDistribution& m_keyDistribution;
            size_t m_requestsDispatched;
            Utilities::TTimestamp m_lastPrint;
            Utilities::TTimestamp m_startTime;
        };
    }
}
