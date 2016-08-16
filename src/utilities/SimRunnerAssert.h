//
//  SimRunnerAssert.h
//  SimRunner
//
//  Created by Scott on 24/09/2014.
//
//

#pragma once

namespace SimRunner
{
    namespace Utilities
    {
        void Assert(const char* file, int line, const char* stringFormat, ...);
        
        void Assert(const char* file, int line);
    }
}

#define ENABLE_ASSERT

#if defined (ENABLE_ASSERT)
    #define SR_ASSERT(x, ...)	if(!(x)) { SimRunner::Utilities::Assert(__FILE__, __LINE__, ##__VA_ARGS__);  }
#else
    #define SR_ASSERT(x, ...)
#endif
