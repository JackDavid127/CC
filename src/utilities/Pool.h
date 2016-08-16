//
//  Pool.h
//  SimRunner
//
//  Created by Scott on 21/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//
#pragma once

#include <string>
#include "SimRunnerAssert.h"

namespace SimRunner
{
    namespace Utilities
    {
        template<typename TPoolItem>
        class PoolDeleter;
        
        template<typename TPoolItem, size_t TPoolSize>
        class PoolSlab
        {
        public:
            typedef TPoolItem PoolItem;
            
            PoolSlab(const std::string& poolName)
            : m_poolName(poolName)
            , m_nextFree(0)
            , m_freeCount(m_entries.size())
            {
                
            }
            
            bool HasFreeEntryAvailable() const
            {
                return m_freeCount != 0;
            }
            
            TPoolItem* Allocate()
            {
                SR_ASSERT(HasFreeEntryAvailable());
                -- m_freeCount;
                
                //printf("Alloc from pool %s: pool has %lu free items left.\n", m_poolName.c_str(), m_freeCount);
                
                TPoolItem* pEntry(&m_entries[m_nextFree]);
                
                for(size_t i = 0; i < m_entries.size(); ++ i)
                {
                    m_nextFree = (m_nextFree + 1) % m_entries.size();
                    if(!m_entries[m_nextFree].IsAllocated())
                    {
                        break;
                    }
                }
                
                return pEntry;
            }
            
            void ReturnToPool(TPoolItem& item)
            {
                ++ m_freeCount;
                item.Release();
            }
            
        private:
            std::array<TPoolItem, TPoolSize> m_entries;
            std::string m_poolName;
            size_t m_freeCount;
            size_t m_nextFree;
        };
        
        template<typename TPoolSlab>
        class PoolDeleter
        {
            typedef typename TPoolSlab::PoolItem TPoolItem;
        public:
            PoolDeleter(TPoolSlab& poolSlab)
            : m_poolSlab(poolSlab)
            {
                
            }
            
            void operator()(TPoolItem* pPoolItem) const
            {
                m_poolSlab.ReturnToPool(*pPoolItem);
            }
            
        private:
            TPoolSlab& m_poolSlab;
        };
    }
}
