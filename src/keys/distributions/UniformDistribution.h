//
//  UniformGenerator.h
//  SimRunner
//
//  Created by Scott on 28/09/2014.
//
//

#pragma once

#include <random>
#include <memory>
#include <boost/noncopyable.hpp>
#include "Keys.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Keys
    {
        namespace Distributions
        {
            template <typename TRandomEngine>
            class UniformDistribution : private boost::noncopyable
            {
            public:
                static std::unique_ptr<UniformDistribution<TRandomEngine> > FromUpperBound(TRandomEngine& randomEngine, KeyType inclusiveUpperBound)
                {
                    return FromRange(randomEngine, 0, inclusiveUpperBound);
                }
                
                static std::unique_ptr<UniformDistribution<TRandomEngine> > FromRange(TRandomEngine& randomEngine, KeyType inclusiveLowerBound, KeyType inclusiveUpperBound)
                {
                    return std::unique_ptr<UniformDistribution<TRandomEngine> >(new UniformDistribution<TRandomEngine>(randomEngine, inclusiveLowerBound, inclusiveUpperBound));
                }
                
                KeyType NextKey()
                {
                    return m_distribution(m_randomEngine);
                }
                
            private:
                UniformDistribution(TRandomEngine& randomEngine, KeyType inclusiveLowerBound, KeyType inclusiveUpperBound)
                : m_inclusiveLowerBound(inclusiveLowerBound)
                , m_exclusiveUpperBound(inclusiveUpperBound + 1)
                , m_randomEngine(randomEngine)
                , m_distribution(m_inclusiveLowerBound, m_exclusiveUpperBound)
                {
                    SR_ASSERT(m_exclusiveUpperBound > m_inclusiveLowerBound);
                    SR_ASSERT(m_exclusiveUpperBound > inclusiveUpperBound); //overflow
                }
                
                KeyType m_inclusiveLowerBound;
                KeyType m_exclusiveUpperBound;
                TRandomEngine m_randomEngine;
                std::uniform_int_distribution<> m_distribution;
            };
        }
    }
}
