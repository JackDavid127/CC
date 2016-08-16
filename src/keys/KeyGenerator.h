//
//  KeyGenerator.h
//  SimRunner
//
//  Created by Scott on 28/09/2014.
//
//

#pragma once

#include <map>
#include <boost/noncopyable.hpp>
#include "Keys.h"
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Keys
    {
        template <typename TDistribution>
        class KeyGenerator : private boost::noncopyable
        {
        public:
            KeyGenerator(TDistribution& keyDistribution, KeyType keySpaceSize)
            : m_keyDistribution(keyDistribution)
            , m_keySpaceSize(keySpaceSize)
            {
                
            }
            
            KeyType NextKey()
            {
                KeyType key = 0;
                
                while(key == 0)
                {
                    key = m_keyDistribution.NextKey();
                }
                
                if(m_consumedKeySpace.find(key) == m_consumedKeySpace.end())
                {
                    m_consumedKeySpace[key] = 0;
                }
                
                m_consumedKeySpace[key] += 1;
                //SR_ASSERT(m_consumedKeySpace.size() <= m_keySpaceSize, "%lu -- %d", m_consumedKeySpace.size(), m_keySpaceSize);
                return key;
            }
            
        private:
            std::map<KeyType, size_t> m_consumedKeySpace;
            TDistribution& m_keyDistribution;
            KeyType m_keySpaceSize;
        };
    }
}
