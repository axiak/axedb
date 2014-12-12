#include "tabletreader.hpp"

namespace dullahan {

using namespace models;

TabletReader::TabletReader(Env *env, const TabletMetadata &tablet_metadata) :
    Tablet(env, tablet_metadata, env->getReadStoreReadingOptions()) {}

TabletReader::TabletReader(TabletReader &&tablet) noexcept :
    Tablet(std::move(tablet)) {
}

TabletReader &TabletReader::operator=(TabletReader &&tablet) noexcept {
  Tablet::operator=(std::move(tablet));
  return *this;
}


} // namespace dullahan