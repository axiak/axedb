#pragma once
#ifndef __DULLAHAN_UTILS_HPP_
#define __DULLAHAN_UTILS_HPP_

#ifndef likely
#define likely(x)       __builtin_expect((x), 1)
#endif

#ifndef unlikely
#define unlikely(x)     __builtin_expect((x), 0)
#endif

#endif // __DULLAHAN_UTILS_HPP_