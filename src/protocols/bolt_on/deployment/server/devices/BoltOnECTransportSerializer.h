//
//  BoltOnECTransportSerializer.h
//  SimRunner
//
//  Created by Scott on 19/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <boost/noncopyable.hpp>
#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <sstream>
#include <string>
#include "document.h"
#include "writer.h"
#include "stringbuffer.h"

namespace SimRunner
{
    namespace Protocols
    {
        namespace BoltOn
        {
            namespace Deployment
            {
                namespace Server
                {
                    namespace Devices
                    {
                        template<typename TProtocolTraits>
                        class BoltOnECTransportSerializer : private boost::noncopyable
                        {
                            //todo optimise: ~20 - 30 % in serialization -- could use binary format encoded to string?
                            
                            typedef typename TProtocolTraits::TStorageKey TStorageKey;
                            typedef typename TProtocolTraits::TBoltOnSerializedValueTypePtr TBoltOnSerializedValueTypePtr;
                            typedef typename TProtocolTraits::TKeyDependencies TKeyDependencies;
                            typedef typename TProtocolTraits::TClientInputValueType TClientInputValueType;
                            typedef typename TProtocolTraits::TCausalClock TCausalClock;
                            typedef typename TProtocolTraits::TKeyDependenciesMapping TKeyDependenciesMapping;
                            typedef typename TProtocolTraits::TShimIdToWritesMapping TShimIdToWritesMapping;
                            typedef typename TProtocolTraits::TShimId TShimId;
                            typedef typename TProtocolTraits::TCounter TCounter;
                            typedef typename TProtocolTraits::TCounterValue TCounterValue;
                            
                        public:
                            void SerializeBoltOnPayloadToJsonString(const TBoltOnSerializedValueTypePtr serializedData, std::string& out_jsonResult)
                            {
                                rapidjson::StringBuffer strbuf;
                                rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
                                
                                writer.StartObject();
                                
                                writer.String("Key");
                                writer.Int(serializedData->Key);
                                
                                writer.String("Value");
                                writer.String(serializedData->Value.c_str());
                                
                                writer.String("CreationTimestamp");
                                writer.Int64(serializedData->CreationTimestamp.count());
                                
                                writer.String("FirstSeenTimestamp");
                                writer.Int64(serializedData->FirstSeenTimestamp.count());
                                
                                writer.String("RevealedTimestamp");
                                writer.Int64(serializedData->RevealedTimestamp.count());
                                
                                SerializeKeyDependencies(writer, serializedData->KeyDependencies);
                                
                                writer.EndObject();
                                out_jsonResult = strbuf.GetString();
                            }
                            
                            void DeserializeBoltOnPayloadFromJsonString(const std::string& jsonResult, TBoltOnSerializedValueTypePtr serializedData)
                            {
                                rapidjson::Document document;
                                
                                if (!document.Parse<0>(jsonResult.c_str()).HasParseError())
                                {
                                    const rapidjson::Value& keyEntry = document["Key"];
                                    serializedData->Key = keyEntry.GetInt();
                                    
                                    const rapidjson::Value& valueEntry = document["Value"];
                                    serializedData->Value = valueEntry.GetString();
                                    
                                    const rapidjson::Value& creationTimestampEntry = document["CreationTimestamp"];
                                    serializedData->CreationTimestamp = Utilities::TTimestamp(creationTimestampEntry.GetInt64());
                                    
                                    const rapidjson::Value& firstSeenTimestampEntry = document["FirstSeenTimestamp"];
                                    serializedData->FirstSeenTimestamp = Utilities::TTimestamp(firstSeenTimestampEntry.GetInt64());
                                    
                                    const rapidjson::Value& revealedTimestampEntry = document["RevealedTimestamp"];
                                    serializedData->RevealedTimestamp = Utilities::TTimestamp(revealedTimestampEntry.GetInt64());
                                    
                                    DeserializeKeyDependencies(document, serializedData->KeyDependencies);
                                }
                            }
                            
                        private:
                            void SerializeKeyDependencies(rapidjson::Writer<rapidjson::StringBuffer>& out_writer,
                                                          const TKeyDependencies& keyDependencies)
                            {
                                const TKeyDependenciesMapping& dependenciesMapping(keyDependencies.Dependencies());
                                
                                out_writer.String("KeyDependencies");
                                
                                out_writer.StartArray();
                                
                                SR_ASSERT(dependenciesMapping.size() >= 1);
                                
                                for (auto dependenciesMappingIt = dependenciesMapping.begin();
                                     dependenciesMappingIt != dependenciesMapping.end();
                                     ++ dependenciesMappingIt)
                                {
                                    const TStorageKey& key(dependenciesMappingIt->first);
                                    const TCausalClock& clock(dependenciesMappingIt->second);
                                    
                                    out_writer.StartObject();
                                    
                                    out_writer.String("Key");
                                    out_writer.Int(key);
                                    
                                    out_writer.String("Clock");
                                    out_writer.StartArray();
                                    
                                    const TShimIdToWritesMapping& shimIdToWritesMapping(clock.Data());
                                    
                                    for (auto shimIdToWritesMappingIt = shimIdToWritesMapping.begin();
                                         shimIdToWritesMappingIt != shimIdToWritesMapping.end();
                                         ++ shimIdToWritesMappingIt)
                                    {
                                        const TShimId& shimId(shimIdToWritesMappingIt->first);
                                        const TCounter& counter(shimIdToWritesMappingIt->second);
                                        
                                        out_writer.StartObject();
                                        
                                        out_writer.String("ShimId");
                                        out_writer.Uint64(shimId);
                                        
                                        out_writer.String("Counter");
                                        out_writer.Uint64(counter.Value());
                                        
                                        out_writer.EndObject();
                                    }
                                    
                                    out_writer.EndArray();
                                    
                                    out_writer.EndObject();
                                }
                                
                                out_writer.EndArray();
                            }
                            
                            void DeserializeKeyDependencies(rapidjson::Document& document,
                                                            TKeyDependencies& out_keyDependencies)
                            {
                                const rapidjson::Value& keyDependenciesJson = document["KeyDependencies"];
                                
                                SR_ASSERT(keyDependenciesJson.Size() >= 1);
                                
                                for(int i = 0; i < keyDependenciesJson.Size(); ++i)
                                {
                                    const rapidjson::Value& keyDependencyJson = keyDependenciesJson[i];
                                    
                                    TStorageKey key(keyDependencyJson["Key"].GetInt());
                                    const rapidjson::Value& clockJson = keyDependencyJson["Clock"];
                                    
                                    TCausalClock clock = TCausalClock::CreateEmpty();
                                    
                                    for(int j = 0; j < clockJson.Size(); ++j)
                                    {
                                        const rapidjson::Value& clockItemJson = clockJson[j];
                                        
                                        TShimId shimId(clockItemJson["ShimId"].GetInt64());
                                        TCounterValue counterValue(clockItemJson["Counter"].GetInt64());
                                        
                                        clock.SetValueForShim(shimId, counterValue);
                                    }
                                    
                                    out_keyDependencies.PutDependency(key, clock);
                                }
                            }
                        };
                    }
                }
            }
        }
    }
}

