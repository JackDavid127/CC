//
//  MetricsStatsWriter.h
//  SimRunner
//
//  Created by Scott on 25/11/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <string>
#include <ostream>
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/stream.hpp>

namespace SimRunner
{
    namespace Metrics
    {
        template<typename ProtocolTraits>
        class MetricsStatsWriter
        {
        public:
            MetricsStatsWriter(const std::string& connectionLogsDirectory)
            : m_connectionLogsDirectory(connectionLogsDirectory)
            {
                
            }
            
            void WriteJson(int32_t metricSourceIdentifier,
                           size_t metricSourceWriteIdentifier,
                           const std::string& statsToWriteJson)
            {
                constexpr auto bufferSize = 64;
                char buffer[bufferSize];
                int result = snprintf(buffer, bufferSize, "%d_%lu.txt", metricSourceIdentifier, metricSourceWriteIdentifier);
                SR_ASSERT(result >= 0);
                SR_ASSERT(result < bufferSize);
                
                WriteJson(buffer, statsToWriteJson);
            }
            
            void WriteJson(const std::string& metricSourceIdentifier,
                           size_t metricSourceWriteIdentifier,
                           const std::string& statsToWriteJson)
            {
                constexpr auto bufferSize = 64;
                char buffer[bufferSize];
                int result = snprintf(buffer, bufferSize, "%s_%lu.txt", metricSourceIdentifier.c_str(), metricSourceWriteIdentifier);
                SR_ASSERT(result >= 0);
                SR_ASSERT(result < bufferSize);
                
                WriteJson(buffer, statsToWriteJson);
            }
            
        private:
            void WriteJson(const std::string& identifier,
                           const std::string& statsToWriteJson)
            {
                std::string path(std::string(m_connectionLogsDirectory) + "/" + identifier);
                
                boost::filesystem::path dir(path);
                
                if(!(boost::filesystem::exists(m_connectionLogsDirectory)))
                {
                    boost::filesystem::create_directories(m_connectionLogsDirectory);
                }
                
                boost::iostreams::stream_buffer<boost::iostreams::file_sink> outBuffer(path);
                std::ostream outStream(&outBuffer);
                outStream << statsToWriteJson;
            }
            
            std::string m_connectionLogsDirectory;
        };
    }
}
