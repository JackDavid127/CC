//
//  Host.h
//  SimRunner
//
//  Created by Scott on 20/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <string>
#include "Protocols.h"

namespace SimRunner
{
    namespace Utilities
    {
        class Host
        {
        public:
            Host(const std::string& hostName)
            :m_hostname(hostName)
            {
                
            }
            
            const std::string& HostName() const
            {
                return m_hostname;
            }
            
        private:
            std::string m_hostname;
        };
    }
}
