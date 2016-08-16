//
//  BoltOnWire.h
//  SimRunner
//
//  Created by Scott on 18/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Wire.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace Deployment
            {
                namespace Wire
                {
                    typedef uint32_t BoltOnMessageHeaderType;
                    constexpr size_t BoltOnMessageHeaderBytes = sizeof(uint32_t);
                    
                    static_assert(BoltOnMessageHeaderBytes == 4, "Expected a ECMessageHeaderType size of 4 bytes, is this platform weird?");
                    
                    enum BoltOnWireMessages : Utilities::Byte
                    {
                        ClientToServerGetRequestMsg,
                        ClientToServerPutRequestMsg,
                        
                        ServerToClientGetResponseMsg,
                        ServerToClientPutResponseMsg
                    };
                    
                    constexpr size_t BoltOnMessagePreamble = /*ECMessageHeaderBytes +*/ sizeof(BoltOnWireMessages);
                }
            }
        }
    }
}
