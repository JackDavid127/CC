//
//  ECServerToServerPutResponse.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "EC.h"
#include "ECWire.h"
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
                    template <typename ProtocolTraits>
                    class ECServerToServerPutResponse
                    {
                        typedef ECServerToServerPutResponse TSelf;
                        typedef Utilities::TTimestamp TTimestamp;
                        typedef SimRunner::Protocols::ClientIdType ClientIdType;
                        
                    public:
                        static TSelf Create(TTimestamp timestamp,
                                            ClientIdType clientId)
                        {
                            return TSelf(timestamp, clientId);
                        }
                        
                        ECWireMessages MessageType() const { return m_messageType; }
                        
                        TTimestamp Timestamp() const { return m_timestamp; }
                        
                        ClientIdType ClientId() const { return m_clientId; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            Utilities::BufferReader<ECMessagePreamble> reader(buffer);
                            TTimestamp timestamp(reader.Read<uint64_t>());
                            ClientIdType clientId = reader.Read<ClientIdType>();
                            return TSelf(timestamp, clientId);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            Utilities::BufferWriter<ECMessageHeaderType>(buffer, BodySize)
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<uint64_t>(m_timestamp.count())
                            .template Write<ClientIdType>(m_clientId);
                        }
                        
                    private:
                        ECServerToServerPutResponse(TTimestamp timestamp,
                                                    ClientIdType clientId)
                        : m_messageType(Wire::ServerToServerPutResponseMsg)
                        , m_timestamp(timestamp)
                        , m_clientId(clientId)
                        {
                            
                        }
                        
                        ECWireMessages m_messageType;
                        TTimestamp m_timestamp;
                        ClientIdType m_clientId;
                        
                        static constexpr ECMessageHeaderType BodySize = sizeof(m_messageType)
                        + sizeof(m_timestamp)
                        + sizeof(m_clientId);
                    };
                }
            }
        }
    }
}
