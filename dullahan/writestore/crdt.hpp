#pragma once
#ifndef __DULLAHAN_WRITESTORE_CRDT_HPP_
#define __DULLAHAN_WRITESTORE_CRDT_HPP_

#include <stdint.h>
#include <string>
#include <algorithm>
#include <ostream>
#include "../protos/dullahan.pb.h"

namespace {
template<typename V>
inline V GetValue(const std::string & value) {
  return *reinterpret_cast<const V *>(value.data());
}

template<typename V>
inline void WriteBytes(V value, std::string * output) {
  output->append(reinterpret_cast<const char *>(&value), sizeof(V));
}
}

namespace dullahan {
namespace writestore {

using namespace ::dullahan::models;

struct Timestamp {
  uint64_t timestampA;
  int32_t timestampB;
};

template<typename V>
class CRDT {
public:
  virtual void Merge(std::string * other) = 0;
  virtual V value() const = 0;
  virtual void Increment(int serverId, V value) = 0;
  virtual void MakeZero() = 0;
  virtual void SerializeToString(std::string * output) const = 0;
  virtual void DebugString(std::ostream & ostream) const = 0;
protected:
  static Timestamp GenerateTimestamp() {
    return nullptr;
  }
};


template<typename V>
class PNCounter: public CRDT<V> {
  static_assert(std::is_arithmetic<V>::value, "PNCounter CRDT only works with numeric types.");


public:

  virtual void Merge(std::string *other) {

  }

  virtual V value() const {
    V total = 0;
    for (auto server_item : pn_counter_.server_items()) {
      total += GetValue<V>(server_item.positive());
      total -= GetValue<V>(server_item.negative());
    }
    return total;
  }

  virtual void Increment(int server_id, V value) {
    auto found_item = std::find_if(
        pn_counter_.mutable_server_items()->begin(),
        pn_counter_.mutable_server_items()->end(),
        [&server_id] (const CRDT_PNCounter_ServerItem & server_item) {
          return server_item.server_id() == server_id;
    });
    if (found_item == pn_counter_.server_items().end()) {
      CRDT_PNCounter_ServerItem * server_item = pn_counter_.add_server_items();
      server_item->set_server_id(server_id);
      if (value > 0) {
        server_item->set_positive(&value, sizeof(V));
      } else {
        server_item->set_negative(&value, sizeof(V));
      }
    } else {
      if (value > 0) {
        found_item->set_positive(&value, sizeof(V));
      } else {
        found_item->set_negative(&value, sizeof(V));
      }
    }
  }

  virtual void MakeZero() {
    for (auto server_item : pn_counter_.server_items()) {
      V total = GetValue<V>(server_item.positive()) + GetValue<V>(server_item.negative());
      server_item.set_positive(&total, sizeof(V));
      server_item.set_negative(&total, sizeof(V));
    }
  }


  virtual void DebugString(std::ostream &ostream) const {
    ostream << "{PNCounter: value=" << value() << ", data=[";
    for (auto server_item : pn_counter_.server_items()) {
      ostream << ""
    }
  }

  virtual void SerializeToString(std::string *output) const {
    pn_counter_.SerializeToString(output);
  }

private:
  ::dullahan::models::CRDT_PNCounter pn_counter_;
};

}
}

#endif // __DULLAHAN_WRITESTORE_CRDT_HPP_