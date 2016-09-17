//
//  KeyValueStorage.h
//  SimRunner
//
//  Created by Scott on 12/10/2014.
//
//

#pragma once

#include <map>
#include <boost/noncopyable.hpp>
#include "SimRunnerAssert.h"
#include "Timing.h"

namespace SimRunner
{
    namespace Components
    {
        template <typename TKey, typename TValue>
        class KeyValueStorage : private boost::noncopyable
        {
            typedef std::map<SimRunner::Utilities::TTimestamp, TValue> TValues;
            typedef std::map<TKey, TValues> TStorage;
            typedef typename TStorage::iterator TStorageIterator;

        public:
            KeyValueStorage() { }

            KeyValueStorage(const KeyValueStorage& other)
            {
                m_storage = other.m_storage;
            }

            bool TryGetValue(const TKey& key, TValue*& out_pValue)
            {
                auto it = m_storage.find(key);

                if(it == m_storage.end())
                {
                    return false;
                }
                else
                {
                    out_pValue = &(it->second.rbegin()->second);
                    return true;
                }
            }

            bool TryGetValue(const TKey& key, TValue*& out_pValue, Utilities::TTimestamp& st, Utilities::TTimestamp& ed)
            {
                auto it = m_storage.find(key);

                if(it == m_storage.end())
                {
                    return false;
                }
                else
                {
                    st = it->second.rbegin()->first;
                    ed = Utilities::Now();
                    out_pValue = &(it->second.rbegin()->second);
                    return true;
                }
            }

            bool TryGetValue(const TKey& key, TValue*& out_pValue, Utilities::TTimestamp tsps)
            {
                auto it = m_storage.find(key);

                if(it == m_storage.end())
                {
                    return false;
                }
                else
                {
                    auto itt = it->second.lower_bound(tsps);
                    if(itt == it->second.end()) return false;
                    out_pValue = &(itt->second);
                    return true;
                }
            }

            void AddValueForKey(const TKey& key, const TValue& value)
            {
                auto findResult(m_storage.find(key));

                if(findResult != m_storage.end())
                {
                    m_storage[key].insert(std::make_pair(Utilities::Now(), value));
                }
                else
                {
                    TValues tmp;
                    tmp.insert(std::make_pair(Utilities::Now(), value));
                    auto insertResult(m_storage.insert(std::make_pair(key, tmp)));
                    // ensure insertion success
                    SR_ASSERT(insertResult.second);
                }
            }

            void Put(const TKey& key, const TValue& value)
            {
                AddValueForKey(key, value);
            }

            TStorageIterator begin() { return m_storage.begin(); }

            TStorageIterator end() { return m_storage.end(); }

            TStorageIterator erase(const TStorageIterator& it) { return m_storage.erase(it); }

            bool empty() const { return m_storage.empty(); }

        private:
            TStorage m_storage;
        };
    }
}
