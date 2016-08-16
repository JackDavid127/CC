//
//  ECWire.h
//  SimRunner
//
//  Created by Scott on 20/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Wire.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace EC
        {
            namespace Deployment
            {
                namespace Wire
                {
                    typedef uint32_t ECMessageHeaderType;
                    constexpr size_t ECMessageHeaderBytes = sizeof(uint32_t);
                    
                    static_assert(ECMessageHeaderBytes == 4, "Expected a ECMessageHeaderType size of 4 bytes, is this platform weird?");
                    
                    enum ECWireMessages : Utilities::Byte
                    {
                        ClientToServerGetRequestMsg,
                        ClientToServerPutRequestMsg,
                        
                        ServerToServerGetRequestMsg,
                        ServerToServerPutRequestMsg,
                        
                        ServerToServerGetResponseMsg,
                        ServerToServerPutResponseMsg,
                        
                        ServerToClientGetResponseMsg,
                        ServerToClientPutResponseMsg,
                        
                        ReplicateMsg,
                        
                        PartitionIdentityExchangeMsg,
                        ReplicaIdentityExchangeMsg,
                    };
                    
                    constexpr size_t ECMessagePreamble = /*ECMessageHeaderBytes +*/ sizeof(ECWireMessages);
                }
            }
        }
    }
}
