//
//  Wire.cpp
//  SimRunner
//
//  Created by Scott on 22/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#include <cstdlib>
#include <boost/asio.hpp>
#include "Wire.h"
#include "Endian.h"

using namespace boost::asio::detail::socket_ops;

namespace SimRunner
{
    namespace Utilities
    {
        int16_t ToHost(int16_t value) { return network_to_host_short(value); }
        int32_t ToHost(int32_t value) { return network_to_host_long(value); }
        int64_t ToHost(int64_t value) { return le64toh(value); }
        
        int16_t ToNetwork(int16_t value) { return host_to_network_short(value); }
        int32_t ToNetwork(int32_t value) { return host_to_network_long(value); }
        int64_t ToNetwork(int64_t value) { return htole64(value); }
        
        uint16_t ToHost(uint16_t value) { return network_to_host_short(value); }
        uint32_t ToHost(uint32_t value) { return network_to_host_long(value); }
        uint64_t ToHost(uint64_t value) { return le64toh(value); }
        
        uint16_t ToNetwork(uint16_t value) { return host_to_network_short(value); }
        uint32_t ToNetwork(uint32_t value) { return host_to_network_long(value); }
        uint64_t ToNetwork(uint64_t value) { return htole64(value); }
    }
}