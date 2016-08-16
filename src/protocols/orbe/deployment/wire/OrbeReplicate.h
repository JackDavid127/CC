//
//  OrbeReplicate.h
//  SimRunner
//
//  Created by Scott on 23/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include "Orbe.h"
#include "OrbeWire.h"
#include "Wire.h"
#include "Timing.h"
#include "OrbeDependencyMatrixParser.h"

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
                    template <typename ProtocolTraits>
                    class OrbeReplicate
                    {
                        typedef OrbeReplicate TSelf;
                        typedef Keys::KeyType TKeyType;
                        typedef typename ProtocolTraits::TClientInputValueType TClientInputValueType;
                        typedef Utilities::TTimestamp TTimestamp;
                        typedef SimRunner::Protocols::ReplicaIdType ReplicaIdType;
                        typedef typename ProtocolTraits::TLogicalTimestamp TLogicalTimestamp;
                        typedef typename ProtocolTraits::TItemDependencyTimestamp TItemDependencyTimestamp;
                        typedef typename ProtocolTraits::TDependencyMatrix TDependencyMatrix;
                        
                    public:
                        static TSelf Create(TKeyType key,
                                            TClientInputValueType value,
                                            TTimestamp timestamp,
                                            ReplicaIdType sourceReplicaId,
                                            const TLogicalTimestamp& logicalTimestamp,
                                            const TItemDependencyTimestamp& itemDependencyTimestamp,
                                            const TDependencyMatrix& dependencyMatrix)
                        {
                            return TSelf(key, value, timestamp, sourceReplicaId, logicalTimestamp, itemDependencyTimestamp, dependencyMatrix);
                        }
                        
                        OrbeWireMessages MessageType() const { return m_messageType; }
                        
                        TKeyType Key() const { return m_key; }
                        
                        TClientInputValueType Value() const { return m_value; }
                        
                        TTimestamp Timestamp() const { return m_timestamp; }
                        
                        ReplicaIdType SourceReplicaId() const { return m_sourceReplicaId; }
                        
                        TLogicalTimestamp LogicalTimestamp() const { return m_logicalTimestamp; }
                        
                        TItemDependencyTimestamp ItemDependencyTimestamp() const { return m_itemDependencyTimestamp; }
                        
                        TDependencyMatrix DependencyMatrix() const { return m_dependencyMatrix; }
                        
                        static TSelf FromByteBuffer(const Utilities::SubBuffer& buffer)
                        {
                            TBufferReader reader(buffer);
                            TKeyType key = reader.Read<TKeyType>();
                            TClientInputValueType value = reader.Read<TClientInputValueType>();
                            TTimestamp timestamp(reader.Read<uint64_t>());
                            ReplicaIdType sourceReplicaId = reader.Read<ReplicaIdType>();
                            TLogicalTimestamp logicalTimestamp = reader.Read<TLogicalTimestamp>();
                            TItemDependencyTimestamp itemDependencyTimestamp = reader.Read<TItemDependencyTimestamp>();
                            TDependencyMatrix dependencyMatrix(ReadDependencyMatrix<ProtocolTraits, TBufferReader>(reader));
                            return TSelf(key, value, timestamp, sourceReplicaId, logicalTimestamp, itemDependencyTimestamp, dependencyMatrix);
                        }
                        
                        void ToBuffer(Utilities::ByteBuffer& buffer) const
                        {
                            typedef Utilities::BufferWriter<OrbeMessageHeaderType> TBufferWriter;
                            
                            const OrbeMessageHeaderType dependencyMatrixSize(static_cast<OrbeMessageHeaderType>(ComputeDependencyMatrixSerializationSize<ProtocolTraits>(m_dependencyMatrix)));
                            
                            const OrbeMessageHeaderType bodySize = sizeof(m_messageType)
                            + sizeof(m_key)
                            + sizeof(m_value)
                            + sizeof(m_timestamp)
                            + sizeof(m_sourceReplicaId)
                            + sizeof(m_logicalTimestamp)
                            + sizeof(m_itemDependencyTimestamp)
                            + dependencyMatrixSize;
                            
                            TBufferWriter writer(buffer, bodySize);
                            
                            writer
                            .template Write<Utilities::Byte>(m_messageType)
                            .template Write<TKeyType>(m_key)
                            .template Write<TClientInputValueType>(m_value)
                            .template Write<uint64_t>(m_timestamp.count())
                            .template Write<ReplicaIdType>(m_sourceReplicaId)
                            .template Write<TLogicalTimestamp>(m_logicalTimestamp)
                            .template Write<TItemDependencyTimestamp>(m_itemDependencyTimestamp);
                            
                            WriteDependencyMatrix<ProtocolTraits, TBufferWriter>(m_dependencyMatrix, writer);
                        }
                        
                    private:
                        OrbeReplicate(TKeyType key,
                                      TClientInputValueType value,
                                      TTimestamp timestamp,
                                      ReplicaIdType sourceReplicaId,
                                      TLogicalTimestamp logicalTimestamp,
                                      TItemDependencyTimestamp itemDependencyTimestamp,
                                      TDependencyMatrix dependencyMatrix)
                        : m_messageType(Wire::ReplicateMsg)
                        , m_key(key)
                        , m_value(value)
                        , m_timestamp(timestamp)
                        , m_sourceReplicaId(sourceReplicaId)
                        , m_logicalTimestamp(logicalTimestamp)
                        , m_itemDependencyTimestamp(itemDependencyTimestamp)
                        , m_dependencyMatrix(dependencyMatrix)
                        {
                            
                        }
                        
                        OrbeWireMessages m_messageType;
                        TKeyType m_key;
                        TClientInputValueType m_value;
                        TTimestamp m_timestamp;
                        ReplicaIdType m_sourceReplicaId;
                        TLogicalTimestamp m_logicalTimestamp;
                        TItemDependencyTimestamp m_itemDependencyTimestamp;
                        TDependencyMatrix m_dependencyMatrix;
                    };
                }
            }
        }
    }
}


