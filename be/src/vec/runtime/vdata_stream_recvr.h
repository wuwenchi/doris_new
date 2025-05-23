// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/task_execution_context.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
#include "common/compile_check_begin.h"
class MemTracker;
class PBlock;
class MemTrackerLimiter;
class RuntimeState;

namespace pipeline {
class Dependency;
class ExchangeLocalState;
} // namespace pipeline

namespace vectorized {
class VDataStreamMgr;
class VSortedRunMerger;

class VDataStreamRecvr;

class VDataStreamRecvr : public HasTaskExecutionCtx {
public:
    class SenderQueue;
    VDataStreamRecvr(VDataStreamMgr* stream_mgr, RuntimeProfile::HighWaterMarkCounter* counter,
                     RuntimeState* state, const TUniqueId& fragment_instance_id,
                     PlanNodeId dest_node_id, int num_senders, bool is_merging,
                     RuntimeProfile* profile, size_t data_queue_capacity);

    ~VDataStreamRecvr() override;

    MOCK_FUNCTION Status create_merger(const VExprContextSPtrs& ordering_expr,
                                       const std::vector<bool>& is_asc_order,
                                       const std::vector<bool>& nulls_first, size_t batch_size,
                                       int64_t limit, size_t offset);

    std::vector<SenderQueue*> sender_queues() const { return _sender_queues; }

    Status add_block(std::unique_ptr<PBlock> pblock, int sender_id, int be_number,
                     int64_t packet_seq, ::google::protobuf::Closure** done,
                     const int64_t wait_for_worker, const uint64_t time_to_find_recvr);

    void add_block(Block* block, int sender_id, bool use_move);

    MOCK_FUNCTION Status get_next(Block* block, bool* eos);

    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    PlanNodeId dest_node_id() const { return _dest_node_id; }

    // Indicate that a particular sender is done. Delegated to the appropriate
    // sender queue. Called from DataStreamMgr.
    void remove_sender(int sender_id, int be_number, Status exec_status);

    void cancel_stream(Status exec_status);

    MOCK_FUNCTION void close();

    // When the source reaches eos = true
    void set_sink_dep_always_ready() const;

    // Careful: stream sender will call this function for a local receiver,
    // accessing members of receiver that are allocated by Object pool
    // in this function is not safe.
    MOCK_FUNCTION bool exceeds_limit(size_t block_byte_size);
    bool queue_exceeds_limit(size_t byte_size) const;
    bool is_closed() const { return _is_closed; }

    std::shared_ptr<pipeline::Dependency> get_local_channel_dependency(int sender_id);

    void set_low_memory_mode() { _sender_queue_mem_limit = 1012 * 1024; }

private:
    friend struct BlockSupplierSortCursorImpl;

    // DataStreamMgr instance used to create this recvr. (Not owned)
    VDataStreamMgr* _mgr = nullptr;

    RuntimeProfile::HighWaterMarkCounter* _memory_used_counter = nullptr;

    std::shared_ptr<ResourceContext> _resource_ctx;

    std::shared_ptr<QueryContext> _query_context;

    // Fragment and node id of the destination exchange node this receiver is used by.
    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // Row schema, copied from the caller of CreateRecvr().
    RowDescriptor _row_desc;

    // True if this reciver merges incoming rows from different senders. Per-sender
    // row batch queues are maintained in this case.
    bool _is_merging;
    bool _is_closed;

    std::unique_ptr<MemTracker> _mem_tracker;
    // Managed by object pool
    std::vector<SenderQueue*> _sender_queues;

    std::atomic<size_t> _sender_queue_mem_limit;

    std::unique_ptr<VSortedRunMerger> _merger;

    ObjectPool _sender_queue_pool;
    RuntimeProfile* _profile = nullptr;

    RuntimeProfile::Counter* _remote_bytes_received_counter = nullptr;
    RuntimeProfile::Counter* _local_bytes_received_counter = nullptr;
    RuntimeProfile::Counter* _deserialize_row_batch_timer = nullptr;
    RuntimeProfile::Counter* _first_batch_wait_total_timer = nullptr;
    RuntimeProfile::Counter* _buffer_full_total_timer = nullptr;
    RuntimeProfile::Counter* _data_arrival_timer = nullptr;
    RuntimeProfile::Counter* _decompress_timer = nullptr;
    RuntimeProfile::Counter* _decompress_bytes = nullptr;

    // Number of blocks received
    RuntimeProfile::Counter* _blocks_produced_counter = nullptr;
    RuntimeProfile::Counter* _max_wait_worker_time = nullptr;
    RuntimeProfile::Counter* _max_wait_to_process_time = nullptr;
    RuntimeProfile::Counter* _max_find_recvr_time = nullptr;

    std::vector<std::shared_ptr<pipeline::Dependency>> _sender_to_local_channel_dependency;
};

class VDataStreamRecvr::SenderQueue {
public:
    SenderQueue(VDataStreamRecvr* parent_recvr, int num_senders,
                std::shared_ptr<pipeline::Dependency> local_channel_dependency);

    ~SenderQueue();

    Status get_batch(Block* next_block, bool* eos);

    Status add_block(std::unique_ptr<PBlock> pblock, int be_number, int64_t packet_seq,
                     ::google::protobuf::Closure** done, const int64_t wait_for_worker,
                     const uint64_t time_to_find_recvr);

    void add_block(Block* block, bool use_move);

    void decrement_senders(int sender_id);

    void cancel(Status cancel_status);

    void close();

    void set_dependency(std::shared_ptr<pipeline::Dependency> dependency) {
        _source_dependency = dependency;
    }

protected:
    void add_blocks_memory_usage(int64_t size);

    void sub_blocks_memory_usage(int64_t size);

    bool exceeds_limit();
    friend class pipeline::ExchangeLocalState;

    void set_source_ready(std::lock_guard<std::mutex>&);

    // To record information about several variables in the event of a DCHECK failure.
    //  DCHECK(_is_cancelled || !_block_queue.empty() || _num_remaining_senders == 0)
#ifndef NDEBUG
    constexpr static auto max_record_number = 128;
    std::list<size_t> _record_block_queue;
    std::list<int> _record_num_remaining_senders;
#else
#endif

    // only in debug
    ALWAYS_INLINE inline void _record_debug_info() {
#ifndef NDEBUG
        if (_record_block_queue.size() > max_record_number) {
            _record_block_queue.pop_front();
        }
        if (_record_num_remaining_senders.size() > max_record_number) {
            _record_num_remaining_senders.pop_front();
        }
        _record_block_queue.push_back(_block_queue.size());
        _record_num_remaining_senders.push_back(_num_remaining_senders);
#else
#endif
    }

    ALWAYS_INLINE inline std::string _debug_string_info() {
#ifndef NDEBUG
        std::stringstream out;
        DCHECK_EQ(_record_block_queue.size(), _record_num_remaining_senders.size());
        out << "record_debug_info [  \n";

        auto it1 = _record_block_queue.begin();
        auto it2 = _record_num_remaining_senders.begin();
        for (; it1 != _record_block_queue.end(); it1++, it2++) {
            out << "( "
                << "_block_queue size : " << *it1 << " , _num_remaining_senders : " << *it2
                << " ) \n";
        }
        out << "  ]\n";
        return out.str();
#else
#endif
        return "";
    }

    // Not managed by this class
    VDataStreamRecvr* _recvr = nullptr;
    std::mutex _lock;
    bool _is_cancelled;
    Status _cancel_status;
    int _num_remaining_senders;
    std::unique_ptr<MemTracker> _queue_mem_tracker;

    // `BlockItem` is used in `_block_queue` to handle both local and remote exchange blocks.
    // For local exchange blocks, `BlockUPtr` is used directly without any modification.
    // For remote exchange blocks, the `pblock` is stored in `BlockItem`.
    // When `getBlock` is called, the `pblock` is deserialized into a usable block.
    struct BlockItem {
        Status get_block(BlockUPtr& block) {
            if (!_block) {
                DCHECK(_pblock);
                SCOPED_RAW_TIMER(&_deserialize_time);
                _block = Block::create_unique();
                RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_block->deserialize(*_pblock));
            }
            block.swap(_block);
            _block.reset();
            return Status::OK();
        }

        size_t block_byte_size() const { return _block_byte_size; }
        int64_t deserialize_time() const { return _deserialize_time; }
        BlockItem() = default;
        BlockItem(BlockUPtr&& block, size_t block_byte_size)
                : _block(std::move(block)), _block_byte_size(block_byte_size) {}

        BlockItem(std::unique_ptr<PBlock>&& pblock, size_t block_byte_size)
                : _block(nullptr), _pblock(std::move(pblock)), _block_byte_size(block_byte_size) {}

    private:
        BlockUPtr _block;
        std::unique_ptr<PBlock> _pblock;
        size_t _block_byte_size = 0;
        int64_t _deserialize_time = 0;
    };

    std::list<BlockItem> _block_queue;

    // sender_id
    std::unordered_set<int> _sender_eos_set;
    // be_number => packet_seq
    std::unordered_map<int, int64_t> _packet_seq_map;
    std::deque<std::pair<google::protobuf::Closure*, MonotonicStopWatch>> _pending_closures;

    std::shared_ptr<pipeline::Dependency> _source_dependency;
    std::shared_ptr<pipeline::Dependency> _local_channel_dependency;
};

} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
