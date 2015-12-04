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

#include "async_query.hpp"

#include "realm_coordinator.hpp"

using namespace realm;
using namespace realm::_impl;

AsyncQuery::AsyncQuery(SortOrder sort,
                       std::unique_ptr<SharedGroup::Handover<Query>> handover,
                       RealmCoordinator& parent)
: m_sort(std::move(sort))
, m_query_handover(std::move(handover))
, parent(parent.shared_from_this())
, m_version(m_query_handover->version)
{
}

void AsyncQuery::add_callback(std::unique_ptr<AsyncQueryCallback> callback)
{
    if (m_error) {
        callback->update_ready();
        return;
    }

    m_callbacks.push_back(std::move(callback));
    m_tv_handover.emplace_back();
}

bool AsyncQuery::remove_callback(realm::AsyncQueryCallback& callback)
{
    for (size_t i = 0; i < m_callbacks.size(); ++i) {
        if (m_callbacks[i].get() != &callback) {
            continue;
        }

        m_callbacks[i] = std::move(m_callbacks.back());
        m_tv_handover[i] = std::move(m_tv_handover.back());
        m_callbacks.pop_back();
        m_tv_handover.pop_back();
        return !m_callbacks.empty();
    }
    REALM_ASSERT(m_error);
    return !m_callbacks.empty();
}

void AsyncQuery::get_results(const SharedRealm& realm, SharedGroup& sg, std::vector<std::function<void()>>& ret)
{
    for (size_t i = 0; i < m_callbacks.size(); ++i) {
        auto& callback = m_callbacks[i];
        if (!callback->is_for_current_thread()) {
            continue;
        }

        if (m_error) {
            auto error = m_error;
            auto cb = callback.release();
            ret.emplace_back([error, cb] {
                std::unique_ptr<AsyncQueryCallback> callback(cb);
                callback->error(error);
            });
            continue;
        }

        auto& tv = m_tv_handover[i];

        if (!tv) {
            continue;
        }
        if (tv->version < sg.get_version_of_current_transaction()) {
            // async results are stale; ignore
            return;
        }
        auto r = Results(realm,
                         m_sort,
                         std::move(*sg.import_from_handover(std::move(tv))));
        auto version = sg.get_version_of_current_transaction();
        // FIXME: what if unregister is called?
        // FIXME: capturing local by ref
        ret.emplace_back([r = std::move(r), version, &sg, self = shared_from_this(), &callback] {
            if (sg.get_version_of_current_transaction() == version) {
                callback->deliver(std::move(r));
            }
        });
    }

    if (m_error) {
        m_callbacks.clear();
        m_tv_handover.clear();
    }
}

void AsyncQuery::prepare_update()
{
    // This function must not touch m_tv_handover as it is called without the
    // relevant lock held (so that another thread can consume m_tv_handover
    // while this is running)

    REALM_ASSERT(m_sg);

    if (m_tv.is_attached()) {
        m_did_update = m_tv.sync_if_needed();
    }
    else {
        m_tv = m_query->find_all();
        if (m_sort) {
            m_tv.sort(m_sort.columnIndices, m_sort.ascending);
        }
        m_did_update = true;
    }
}

void AsyncQuery::prepare_handover()
{
    for (size_t i = 0; i < m_callbacks.size(); ++i) {
        auto& callback = *m_callbacks[i];
        auto& tv = m_tv_handover[i];

        // Even if the TV didn't change, we need to re-export it if the previous
        // export has not been consumed yet, as the old handover object is no longer
        // usable due to the version not matching
        if (m_did_update || callback.first_run || (tv && tv->version != m_sg->get_version_of_current_transaction())) {
            tv = m_sg->export_for_handover(m_tv, ConstSourcePayload::Copy);
            callback.first_run = false;
        }

        if (m_did_update) {
            callback.update_ready();
        }
    }

    m_version = m_sg->get_version_of_current_transaction();
}

void AsyncQuery::set_error(std::exception_ptr err)
{
    if (!m_error) {
        m_error = err;
        for (auto& callback : m_callbacks)
            callback->update_ready();
    }
}

void AsyncQuery::attach_to(realm::SharedGroup& sg)
{
    REALM_ASSERT(!m_sg);

    m_query = sg.import_from_handover(std::move(m_query_handover));
    m_sg = &sg;
}

void AsyncQuery::detatch()
{
    REALM_ASSERT(m_sg);

    m_query_handover = m_sg->export_for_handover(*m_query, MutableSourcePayload::Move);
    m_sg = nullptr;
}
