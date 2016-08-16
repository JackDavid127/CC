//
//  StringValueParser.h
//  SimRunner
//
//  Created by Scott on 19/12/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

namespace SimRunner
{
    namespace Utilities
    {
        template <typename TString = typename std::string>
        size_t ComputeStringSerializationSize(const TString& string)
        {
            return sizeof(uint32_t) + string.size() + 1; //null term
        }
        
        template <
        typename TBufferReader,
        typename TString = typename std::string>
        TString ReadString(TBufferReader& reader)
        {
            uint32_t size(reader.template Read<uint32_t>());
            TString string;
            string.resize(size - 1);//null term
            reader.template ReadByteStream(&string[0], string.size());
            return string;
        }
        
        template <
        typename TBufferWriter,
        typename TString = typename std::string>
        void WriteString(const TString& string, TBufferWriter& writer)
        {
            writer.template Write<uint32_t>(static_cast<uint32_t>(string.size() + 1)); //null term
            writer.template WriteByteStream(string.data(), string.size());
        }
    }
}
