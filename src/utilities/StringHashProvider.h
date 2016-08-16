//
//  StringHashProvider.h
//  SimRunner
//
//  Created by Scott on 12/10/2014.
//
//

#pragma once

#include <boost/noncopyable.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/functional/hash.hpp>
#include "Protocols.h"

namespace SimRunner
{
    namespace Utilities
    {
        class NumericStringHashProvider : private boost::noncopyable
        {
        public:
            template<typename THashResult>
            inline THashResult Hash(const std::string& value) const
            {
                return boost::lexical_cast<THashResult>(value);
            }
            
            template<typename THashResult>
            inline THashResult Hash(const int& value) const
            {
                return boost::lexical_cast<THashResult>(value);
            }
        };
        
        class GenericStringHashProvider : private boost::noncopyable
        {
        public:
            template<typename THashResult>
            inline THashResult Hash(const std::string& value) const
            {
                boost::hash<std::string> stringHash;
                return boost::lexical_cast<THashResult>(stringHash(value));
            }
        };
    }
}
