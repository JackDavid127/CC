//
//  Timing.h
//  SimRunner
//
//  Created by Scott on 17/10/2014.
//  Copyright (c) 2014 Scott. All rights reserved.
//

#pragma once

#include <boost/chrono.hpp>
#include <stdio.h>

namespace SimRunner
{
    namespace Utilities
    {
        typedef boost::chrono::microseconds TTimestamp;
        typedef boost::chrono::microseconds TTimeDelta;

        typedef boost::chrono::seconds Seconds;

        TTimestamp Now()
        {
            boost::chrono::system_clock::time_point now(boost::chrono::system_clock::now());
            boost::chrono::microseconds micros = boost::chrono::duration_cast<boost::chrono::microseconds>(now.time_since_epoch());
            //printf("Timestamps::Now()\n");
            return micros;
        }

        template<typename TDurationType>
        TDurationType GetDeltaTime(const TTimestamp& first, const TTimestamp& second)
        {
            //printf("Timestamps::GetDeltaTime()\n");
            return boost::chrono::duration_cast<TDurationType>(first - second);
        }

        TTimeDelta GetDeltaTime(const TTimestamp& first, const TTimestamp& second)
        {
            return GetDeltaTime<TTimeDelta>(first, second);
        }
    }
}
