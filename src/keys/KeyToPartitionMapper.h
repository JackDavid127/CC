//
//  KeyToPartitionMapper.h
//  SimRunner
//
//  Created by Scott on 12/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include <cmath>
#include "Protocols.h"

namespace SimRunner
{
    namespace Keys
    {
        template <typename TKey, typename TKeyHashProvider>
        class KeyToPartitionMapper : private boost::noncopyable
        {
            typedef Protocols::PartitionIdType TPartitionIdType;
            
        public:
            KeyToPartitionMapper(TPartitionIdType partitionsCount, const TKeyHashProvider& keyHasher)
            : m_partitionsCount(partitionsCount)
            , m_keyHasher(keyHasher)
            {
                
            }
            
            TPartitionIdType PartitionForKey(const TKey& key)
            {
                //TPartitionIdType keyHash(m_keyHasher.template Hash<TPartitionIdType>(key));
                //TPartitionIdType partitionServerId = keyHash % m_partitionsCount;
                
                TPartitionIdType partitionServerId(std::abs(static_cast<TPartitionIdType>(key % m_partitionsCount)));
                return partitionServerId;
            }
            
        private:
            TPartitionIdType m_partitionsCount;
            const TKeyHashProvider& m_keyHasher;
        };
    }
}
