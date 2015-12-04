////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_ASYNC_QUERY_HPP
#define REALM_ASYNC_QUERY_HPP

#include "results.hpp"

#include <realm/group_shared.hpp>

#include <functional>

namespace realm {
namespace _impl {
class AsyncQuery : public std::enable_shared_from_this<AsyncQuery> {
public:
    AsyncQuery(SortOrder sort,
               std::unique_ptr<SharedGroup::Handover<Query>> handover,
               RealmCoordinator& parent);

    void add_callback(std::unique_ptr<AsyncQueryCallback>);
    bool remove_callback(AsyncQueryCallback& callback);

    void get_results(const SharedRealm& realm, SharedGroup& sg, std::vector<std::function<void()>>& ret);

    void set_error(std::exception_ptr err);

    // Run/rerun the query if needed
    void prepare_update();
    // Update the handover object with the new data produced in prepare_update()
    void prepare_handover();

    // Get the version of the current handover object
    SharedGroup::VersionID version() const noexcept { return m_version; }

    void attach_to(SharedGroup& sg);
    void detatch();

    std::shared_ptr<RealmCoordinator> parent;

private:
    const SortOrder m_sort;

    std::unique_ptr<SharedGroup::Handover<Query>> m_query_handover;
    std::unique_ptr<Query> m_query;

    TableView m_tv;

    // FIXME: use
    struct CallbackInfo {
        std::unique_ptr<AsyncQueryCallback> callback;
        std::unique_ptr<SharedGroup::Handover<TableView>> tv;
        bool has_delivered_first = false;
        bool has_delivered_error = false;
    };
    std::vector<std::unique_ptr<AsyncQueryCallback>> m_callbacks;
    std::vector<> m_tv_handover;

    SharedGroup::VersionID  m_version;

    SharedGroup* m_sg = nullptr;

    std::exception_ptr m_error;

    bool m_did_update = false;
};

} // namespace _impl
} // namespace realm

#endif /* REALM_ASYNC_QUERY_HPP */
