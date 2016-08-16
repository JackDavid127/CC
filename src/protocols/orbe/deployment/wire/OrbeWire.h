//
//  OrbeWire.h
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
        namespace Orbe
        {
            namespace Deployment
            {
                namespace Wire
                {
                    typedef uint32_t OrbeMessageHeaderType;
                    constexpr size_t OrbeMessageHeaderBytes = sizeof(uint32_t);
                    
                    static_assert(OrbeMessageHeaderBytes == 4, "Expected a OrbeMessageHeaderType size of 4 bytes, is this platform weird?");
                    
                    enum OrbeWireMessages : Utilities::Byte
                    {
                        ClientToServerGetRequestMsg,
                        ClientToServerPutRequestMsg,
                        
                        ServerToServerGetRequestMsg,
                        ServerToServerPutRequestMsg,
                        
                        ServerToServerGetResponseMsg,
                        ServerToServerPutResponseMsg,
                        
                        ServerToClientGetResponseMsg,
                        ServerToClientPutResponseMsg,
                        
                        VersionVectorCheckRequestMsg,
                        VersionVectorCheckResponseMsg,
                        
                        ReplicateMsg,
                        
                        PartitionIdentityExchangeMsg,
                        ReplicaIdentityExchangeMsg,
                        
                        ManualClientStatusMsg
                    };
                    
                    constexpr size_t OrbeMessagePreamble = /*OrbeMessageHeaderBytes + */ sizeof(OrbeWireMessages);
                    
                    typedef Utilities::BufferReader<OrbeMessagePreamble> TBufferReader;
                }
            }
        }
    }
}
