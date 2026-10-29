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

#include "exec/short_circuit_hybrid.h"

#include <deque>
#include <future>

#include "base/string/slice.h"
#include "column/chunk_factory.h"
#include "column/column_helper.h"
#include "column/datum_convert.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/util/thrift_util.h"
#include "data_sink/result/memory_scratch_sink.h"
#include "exec/exec_env.h"
#include "exec/scan_node.h"
#include "exprs/chunk_predicate_evaluator.h"
#include "exprs/expr.h"
#include "exprs/expr_executor.h"
#include "exprs/expr_factory.h"
#include "fs/fs_factory.h"
#include "runtime/chunk_helper.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/del_vector.h"
#include "storage/rowset/bitmap_index_evaluator.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/options.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/segment.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"
#include "storage/update_manager.h"
#include "storage_primitive/chunk_iterator.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/predicate_tree/predicate_tree.h"
#include "storage_primitive/roaring2range.h"
#include "types/type_info.h"

namespace starrocks {

// bound on rows a secondary-index short circuit may return; mirrors FE
// ShortCircuitPlanner.MAX_RETURN_ROWS so an over-selective predicate errors
// out instead of streaming an unbounded scan through the short path
static constexpr int64_t MAX_SECONDARY_SCAN_ROWS = 2048;

Status ShortCircuitHybridScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

Status ShortCircuitHybridScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    return Status::OK();
}

Status ShortCircuitHybridScanNode::open(RuntimeState* state) {
    _t_desc_tbl = &_common_request.desc_tbl;
    _key_literal_exprs = &_common_request.key_literal_exprs;
    _versions.swap(_common_request.versions);

    // get tablet
    for (auto tablet_id : _common_request.tablet_ids) {
        auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        if (tablet == nullptr) {
            return Status::NotFound(fmt::format("tablet {} not exist", tablet_id));
        }
        _tablets.emplace_back(std::move(tablet));
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _secondary_mode = !_common_request.__isset.key_literal_exprs || _key_literal_exprs->empty();
    _num_rows = _key_literal_exprs->size();
    //init tuple
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    DCHECK(_tuple_desc != nullptr);

    RETURN_IF_ERROR(ExprExecutor::open(_conjunct_ctxs, state));
    if (_secondary_mode) {
        RETURN_IF_ERROR(_build_secondary_predicates(state));
    }
    return Status::OK();
}

Status ShortCircuitHybridScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    if (*eos) {
        return Status::OK();
    }
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (_secondary_mode) {
        return _get_next_secondary(state, chunk, eos);
    }
    std::vector<bool> found(_num_rows, false);
    Buffer<uint8_t> selections;

    RETURN_IF_ERROR(_process_key_chunk());
    RETURN_IF_ERROR(_process_value_chunk(found));
    size_t result_size = 0;
    for (int i = 0; i < found.size(); ++i) {
        if (found[i]) {
            result_size++;
            selections.emplace_back(1);
        } else {
            selections.emplace_back(0);
        }
    }

    auto tablet_schema = _tablets[0]->tablet_schema()->schema();
    auto column_ids = tablet_schema->field_column_ids();
    auto tablet_schema_without_rowstore = std::make_unique<Schema>(tablet_schema, column_ids);
    auto result_chunk = RuntimeChunkHelper::new_chunk(*_tuple_desc, result_size);

    //idx is column id, value is slot id
    if (result_size > 0) {
        _key_chunk->filter(selections);
        for (auto slot_desc : _tuple_desc->slots()) {
            auto field = tablet_schema_without_rowstore->get_field_by_name(slot_desc->col_name());
            if (field->is_key()) {
                result_chunk->get_column_raw_ptr_by_slot_id(slot_desc->id())
                        ->append(*(_key_chunk->get_column_by_name(field->name().data()).get()));
            }
        }

        for (auto slot_desc : _tuple_desc->slots()) {
            auto field = tablet_schema_without_rowstore->get_field_by_name(slot_desc->col_name());
            if (!field->is_key()) {
                result_chunk->get_column_raw_ptr_by_slot_id(slot_desc->id())
                        ->append(*(_value_chunk->get_column_by_name(field->name().data()).get()));
            }
        }
        RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(_conjunct_ctxs, result_chunk.get()));
        if (result_chunk->num_rows() == 0) {
            *eos = true;
        }
    } else {
        *eos = true;
    }
    *chunk = std::move(result_chunk);
    _num_rows_returned += (*chunk)->num_rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

Status ShortCircuitHybridScanNode::_process_key_chunk() {
    DCHECK(_tablets.size() > 0);
    _tablet_schema = _tablets[0]->tablet_schema();
    vector<uint32_t> pk_columns;
    pk_columns.reserve(_tablet_schema->num_key_columns());
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    auto key_schema = ChunkHelper::convert_schema(_tablet_schema, pk_columns);

    _key_chunk = ChunkFactory::new_chunk(key_schema, _num_rows);
    _key_chunk->reset();

    for (int i = 0; i < _num_rows; ++i) {
        // TODO (jkj) if expr is k1=1 and k2 in (3, 4), we need bind tablet with expr,
        // tablet 1  <---> k1 =1, k2 =3
        // tablet 2  <---> k1 =1, k2 =4
        // this prune need happen in fe
        auto keys_literal_expr = (*_key_literal_exprs)[i].literal_exprs;
        size_t num_pk_filters = keys_literal_expr.size();
        // must all columns
        if (UNLIKELY(num_pk_filters != _tablet_schema->num_key_columns())) {
            return Status::Corruption("short circuit only support all key predicate");
        }
        for (int j = 0; j < num_pk_filters; ++j) {
            // init expr context
            std::vector<ExprContext*> expr_ctxs;
            std::vector<TExpr> key_literal_expr{keys_literal_expr[j]};
            // prepare
            RETURN_IF_ERROR(ExprFactory::create_expr_trees(runtime_state()->obj_pool(), key_literal_expr, &expr_ctxs,
                                                           runtime_state()));
            RETURN_IF_ERROR(ExprExecutor::prepare(expr_ctxs, runtime_state()));
            RETURN_IF_ERROR(ExprExecutor::open(expr_ctxs, runtime_state()));
            auto& iteral_expr_ctx = expr_ctxs[0];
            ASSIGN_OR_RETURN(ColumnPtr value, iteral_expr_ctx->root()->evaluate_const(iteral_expr_ctx));
            if (UNLIKELY(value == nullptr || value->only_null() || value->is_null(0))) {
                return Status::EndOfFile("iteral_expr_ctx evaluated to null, won’t execute here");
            }
            // add const column to chunk
            auto const_column = ColumnHelper::get_data_column(value.get());
            _key_chunk->get_column_raw_ptr_by_index(j)->append(*const_column);
        }
    }

    return Status::OK();
}

// subtree size of the node rooted at |idx| in a pre-order TExpr node list
static size_t _expr_subtree_size(const std::vector<TExprNode>& nodes, size_t idx) {
    size_t size = 1;
    for (int i = 0; i < nodes[idx].num_children; ++i) {
        size += _expr_subtree_size(nodes, idx + size);
    }
    return size;
}

Status ShortCircuitHybridScanNode::_literal_to_storage_string(RuntimeState* state, const std::vector<TExprNode>& nodes,
                                                              size_t idx, size_t size, std::string* out) {
    TExpr literal_texpr;
    literal_texpr.nodes.assign(nodes.begin() + idx, nodes.begin() + idx + size);
    std::vector<ExprContext*> expr_ctxs;
    RETURN_IF_ERROR(ExprFactory::create_expr_trees(state->obj_pool(), {literal_texpr}, &expr_ctxs, state));
    RETURN_IF_ERROR(ExprExecutor::prepare(expr_ctxs, state));
    RETURN_IF_ERROR(ExprExecutor::open(expr_ctxs, state));
    ASSIGN_OR_RETURN(ColumnPtr value, expr_ctxs[0]->root()->evaluate_const(expr_ctxs[0]));
    if (UNLIKELY(value == nullptr || value->only_null() || value->is_null(0))) {
        return Status::EndOfFile("secondary short circuit literal evaluated to null");
    }
    TypeInfoPtr literal_type = get_type_info(expr_ctxs[0]->root()->type());
    Datum datum = value->get(0);
    *out = datum_to_string(literal_type.get(), datum);
    return Status::OK();
}

Status ShortCircuitHybridScanNode::_build_secondary_predicates(RuntimeState* state) {
    const Schema* schema = _tablets[0]->tablet_schema()->schema();
    for (const TExpr& conj : _t_node.conjuncts) {
        const std::vector<TExprNode>& nodes = conj.nodes;
        if (nodes.empty()) {
            return Status::NotSupported("secondary short circuit: empty conjunct");
        }
        const TExprNode& root = nodes[0];
        size_t slot_idx = 0;
        // pooled: varchar predicates keep a Slice into the string memory
        std::vector<std::string>* operand_strings = _secondary_pred_pool.add(new std::vector<std::string>());
        if (root.node_type == TExprNodeType::BINARY_PRED) {
            if (root.opcode != TExprOpcode::EQ || root.num_children != 2) {
                return Status::NotSupported("secondary short circuit only supports equality predicates");
            }
            size_t left_size = _expr_subtree_size(nodes, 1);
            if (nodes[1].node_type == TExprNodeType::SLOT_REF) {
                slot_idx = 1;
                RETURN_IF_ERROR(_literal_to_storage_string(state, nodes, 1 + left_size, nodes.size() - 1 - left_size,
                                                           &operand_strings->emplace_back()));
            } else if (nodes[1 + left_size].node_type == TExprNodeType::SLOT_REF) {
                slot_idx = 1 + left_size;
                RETURN_IF_ERROR(
                        _literal_to_storage_string(state, nodes, 1, left_size, &operand_strings->emplace_back()));
            } else {
                return Status::NotSupported("secondary short circuit: equality has no column side");
            }
        } else if (root.node_type == TExprNodeType::IN_PRED) {
            if (root.in_predicate.is_not_in || nodes[1].node_type != TExprNodeType::SLOT_REF) {
                return Status::NotSupported("secondary short circuit only supports IN predicates on a column");
            }
            slot_idx = 1;
            size_t idx = 2;
            while (idx < nodes.size()) {
                size_t child_size = _expr_subtree_size(nodes, idx);
                // NULL can never satisfy IN(...); skip null elements so e.g.
                // col IN ('a', NULL) still returns the 'a' matches.
                Status st = _literal_to_storage_string(state, nodes, idx, child_size, &operand_strings->emplace_back());
                if (st.is_end_of_file()) {
                    operand_strings->pop_back();
                } else {
                    RETURN_IF_ERROR(st);
                }
                idx += child_size;
            }
            if (operand_strings->empty()) {
                return Status::EndOfFile("secondary short circuit: IN list is all NULL");
            }
        } else {
            return Status::NotSupported("secondary short circuit only supports =/IN literal predicates");
        }

        SlotDescriptor* slot_desc = state->desc_tbl().get_slot_descriptor(nodes[slot_idx].slot_ref.slot_id);
        if (slot_desc == nullptr) {
            return Status::NotFound("secondary short circuit: slot not found");
        }
        FieldPtr field = schema->get_field_by_name(slot_desc->col_name());
        if (field == nullptr) {
            return Status::NotFound(
                    fmt::format("secondary short circuit: column {} not in tablet schema", slot_desc->col_name()));
        }
        ColumnPredicate* predicate = nullptr;
        if (root.node_type == TExprNodeType::BINARY_PRED) {
            predicate = new_column_eq_predicate(field->type(), field->id(), Slice((*operand_strings)[0]));
        } else {
            predicate = new_column_in_predicate(field->type(), field->id(), *operand_strings);
        }
        if (predicate == nullptr) {
            return Status::NotSupported("secondary short circuit: failed to build column predicate");
        }
        _secondary_pred_pool.add(predicate);
        _secondary_predicates.emplace_back(predicate);
    }
    if (_secondary_predicates.empty()) {
        return Status::NotSupported("secondary short circuit requires at least one predicate");
    }
    return Status::OK();
}

Status ShortCircuitHybridScanNode::_process_scan_chunk() {
    DCHECK(!_tablets.empty());
    _tablet_schema = _tablets[0]->tablet_schema();
    std::vector<std::string> value_field_names;
    std::vector<starrocks::ColumnId> value_column_ids;
    for (auto slot_desc : _tuple_desc->slots()) {
        FieldPtr field = _tablet_schema->schema()->get_field_by_name(slot_desc->col_name());
        if (field == nullptr) {
            return Status::NotFound(
                    fmt::format("secondary short circuit: column {} not in tablet schema", slot_desc->col_name()));
        }
        value_field_names.emplace_back(field->name());
        value_column_ids.emplace_back(field->id());
    }
    auto value_schema = std::make_unique<Schema>(_tablet_schema->schema(), value_column_ids);
    _value_chunk = ChunkFactory::new_chunk(*value_schema, MAX_SECONDARY_SCAN_ROWS + 1);

    // per-tablet scans are independent; run them in parallel so latency is
    // max-per-tablet rather than the serial sum (the normal pipeline gets this
    // for free from driver parallelism, the short path has to do it by hand).
    // std::async threads are uncapped, so bound the in-flight count to keep a
    // high-bucket table from spawning a thread per tablet under concurrency.
    constexpr size_t MAX_PARALLEL_TABLET_SCANS = 8;
    std::deque<std::pair<size_t, std::future<StatusOr<ChunkPtr>>>> inflight;
    for (size_t i = 0; i < _tablets.size(); ++i) {
        inflight.emplace_back(
                i, std::async(std::launch::async, [this, i, &value_field_names, &value_column_ids, &value_schema]() {
                    return _scan_one_tablet(_tablets[i], _versions[i], value_field_names, value_column_ids,
                                            *value_schema);
                }));
        if (inflight.size() >= MAX_PARALLEL_TABLET_SCANS) {
            ASSIGN_OR_RETURN(ChunkPtr tablet_chunk, inflight.front().second.get());
            if (_value_chunk->num_rows() + tablet_chunk->num_rows() > MAX_SECONDARY_SCAN_ROWS) {
                return Status::InternalError(fmt::format("secondary short circuit matched more than {} rows, aborting",
                                                         MAX_SECONDARY_SCAN_ROWS));
            }
            _value_chunk->append(*tablet_chunk);
            inflight.pop_front();
        }
    }
    for (auto& [i, scan] : inflight) {
        ASSIGN_OR_RETURN(ChunkPtr tablet_chunk, scan.get());
        if (_value_chunk->num_rows() + tablet_chunk->num_rows() > MAX_SECONDARY_SCAN_ROWS) {
            return Status::InternalError(fmt::format("secondary short circuit matched more than {} rows, aborting",
                                                     MAX_SECONDARY_SCAN_ROWS));
        }
        _value_chunk->append(*tablet_chunk);
    }
    _num_rows = _value_chunk->num_rows();
    return Status::OK();
}

// Resolve the secondary predicates through each segment's bitmap index into
// segment-local rowids, then fetch the matching rows by rowid: via
// TabletUpdates::get_column_values on PK tablets (the multi_get tail, which
// reads the __row blob on COLUMN_WITH_ROW tablets) or per-segment column
// iterators on DUPLICATE tablets. This avoids the per-segment iterator setup
// that TableReader::scan pays and is what makes a ~2ms point read possible.
// Returns NotSupported when any segment cannot serve the predicates from its
// bitmap index, so the caller falls back.
StatusOr<ChunkPtr> ShortCircuitHybridScanNode::_scan_one_tablet_by_bitmap_rowids(
        const TabletSharedPtr& tablet, const std::string& version,
        const std::vector<starrocks::ColumnId>& value_column_ids, const Schema& value_schema) {
    int64_t read_version = std::stol(version);
    const auto& tablet_schema = tablet->tablet_schema();

    PredicateAndNode and_node;
    for (const auto* pred : _secondary_predicates) {
        and_node.add_child(PredicateColumnNode{pred});
    }
    PredicateTree pred_tree = PredicateTree::create(std::move(and_node));

    std::vector<RowsetSharedPtr> rowsets;
    RETURN_IF_ERROR(tablet->capture_consistent_rowsets(Version(0, read_version), &rowsets));

    std::unordered_map<ColumnId, ColumnUID> cid_2_ucid;
    for (const auto& field : tablet_schema->schema()->fields()) {
        cid_2_ucid[field->id()] = field->uid();
    }

    const bool is_primary_keys = tablet->updates() != nullptr;
    std::shared_ptr<FileSystem> fs;
    LocalDelvecLoader delvec_loader(tablet->data_dir()->get_meta());
    OlapReaderStatistics stats;
    std::map<uint32_t, std::vector<uint32_t>> rowids_by_rssid;
    std::unordered_map<uint32_t, SegmentSharedPtr> rssid_to_segment;
    size_t total_rows = 0;
    for (const auto& rowset : rowsets) {
        // Rowset-level delete predicates are applied at scan time and the
        // bitmap index is not aware of them; rows deleted this way would leak
        // through the rowid fetch, so take the full scan path instead.
        if (rowset->rowset_meta()->has_delete_predicate()) {
            return Status::NotSupported("secondary short circuit: rowset has delete predicates");
        }
        RETURN_IF_ERROR(rowset->load());
        for (size_t seg_id = 0; seg_id < rowset->num_segments(); ++seg_id) {
            const auto& segment = rowset->segments()[seg_id];
            const uint32_t rssid = rowset->rowset_meta()->get_rowset_seg_id() + seg_id;
            if (fs == nullptr) {
                ASSIGN_OR_RETURN(fs, FileSystemFactory::CreateSharedFromString(rowset->rowset_path()));
            }
            RandomAccessFileOptions ropts;
            if (segment->encryption_info()) {
                ropts.encryption_info = *segment->encryption_info();
            }
            ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(ropts, segment->file_info()));

            BitmapIndexEvaluator evaluator(*tablet_schema->schema(), pred_tree);
            RETURN_IF_ERROR(evaluator.init([&](ColumnId cid) -> StatusOr<BitmapIndexIterator*> {
                IndexReadOptions opts;
                opts.use_page_cache = true;
                opts.read_file = read_file.get();
                opts.stats = &stats;
                opts.tablet_id = tablet->tablet_id();
                opts.segment_id = rssid;
                opts.query_version = read_version;
                opts.col_unique_id = cid_2_ucid[cid];
                BitmapIndexIterator* iter = nullptr;
                RETURN_IF_ERROR(segment->new_bitmap_index_iterator(cid_2_ucid[cid], opts, &iter));
                return iter;
            }));
            if (!evaluator.has_bitmap_index()) {
                return Status::NotSupported("secondary short circuit: segment has no bitmap index");
            }
            SparseRange<> scan_range;
            scan_range.add(Range<rowid_t>(0, segment->num_rows()));
            PredicateTree residual_tree;
            RETURN_IF_ERROR(evaluator.evaluate(scan_range, residual_tree));
            if (!residual_tree.empty()) {
                return Status::NotSupported("secondary short circuit: predicate not covered by bitmap index");
            }
            if (scan_range.empty()) {
                continue;
            }
            Roaring row_bitmap = range2roaring(scan_range);
            if (is_primary_keys && read_version > 0) {
                DelVectorPtr delvec;
                RETURN_IF_ERROR(delvec_loader.load(TabletSegmentId(tablet->tablet_id(), rssid), read_version, &delvec));
                if (delvec != nullptr && !delvec->empty()) {
                    row_bitmap -= *(delvec->roaring());
                }
            }
            std::vector<uint32_t>& rowids = rowids_by_rssid[rssid];
            rowids.reserve(row_bitmap.cardinality());
            for (uint32_t rowid : row_bitmap) {
                rowids.push_back(rowid);
            }
            rssid_to_segment[rssid] = segment;
            total_rows += rowids.size();
            if (total_rows > MAX_SECONDARY_SCAN_ROWS) {
                return Status::InternalError(fmt::format("secondary short circuit matched more than {} rows, aborting",
                                                         MAX_SECONDARY_SCAN_ROWS));
            }
        }
    }

    ChunkPtr tablet_chunk = ChunkFactory::new_chunk(value_schema, total_rows + 1);
    if (total_rows == 0) {
        return tablet_chunk;
    }

    // read columns in column-id order for page locality, then map back to the
    // requested slot order (same idiom as multi_get)
    std::vector<std::pair<uint32_t, uint32_t>> value_column_ids_by_order_with_orig_idx;
    value_column_ids_by_order_with_orig_idx.reserve(value_column_ids.size());
    for (uint32_t i = 0; i < value_column_ids.size(); ++i) {
        value_column_ids_by_order_with_orig_idx.emplace_back(value_column_ids[i], i);
    }
    std::sort(value_column_ids_by_order_with_orig_idx.begin(), value_column_ids_by_order_with_orig_idx.end());
    std::vector<uint32_t> sorted_column_ids;
    sorted_column_ids.reserve(value_column_ids_by_order_with_orig_idx.size());
    for (const auto& p : value_column_ids_by_order_with_orig_idx) {
        sorted_column_ids.push_back(p.first);
    }
    auto read_column_schema = ChunkHelper::convert_schema(tablet_schema, sorted_column_ids);
    MutableColumns read_columns(sorted_column_ids.size());
    for (uint32_t i = 0; i < read_columns.size(); ++i) {
        read_columns[i] = ChunkFactory::column_from_field(*read_column_schema.field(i))->clone_empty();
    }
    if (is_primary_keys) {
        RETURN_IF_ERROR(tablet->updates()->get_column_values(sorted_column_ids, read_version, false, rowids_by_rssid,
                                                             &read_columns, nullptr, tablet_schema));
    } else {
        // Non-PK tablets have no TabletUpdates; fetch each column by rowid
        // directly from the already-open segments (the same primitive
        // get_column_values uses internally). Rows come back in
        // rssid-sorted order, identical across columns.
        for (const auto& [rssid, rowids] : rowids_by_rssid) {
            const auto& segment = rssid_to_segment[rssid];
            RandomAccessFileOptions ropts;
            if (segment->encryption_info()) {
                ropts.encryption_info = *segment->encryption_info();
            }
            ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(ropts, segment->file_info()));
            ColumnIteratorOptions iter_opts;
            iter_opts.stats = &stats;
            iter_opts.use_page_cache = true;
            iter_opts.read_file = read_file.get();
            for (uint32_t i = 0; i < sorted_column_ids.size(); ++i) {
                ASSIGN_OR_RETURN(auto col_iter, segment->new_column_iterator_or_default(
                                                        tablet_schema->column(sorted_column_ids[i]), nullptr));
                RETURN_IF_ERROR(col_iter->init(iter_opts));
                RETURN_IF_ERROR(col_iter->fetch_values_by_rowid(rowids.data(), rowids.size(), read_columns[i].get()));
                const auto& field = tablet_schema->schema()->field(sorted_column_ids[i]);
                if (field->type()->type() == TYPE_CHAR) {
                    ChunkHelper::padding_char_column(tablet_schema, *field, read_columns[i].get());
                }
            }
        }
    }
    for (size_t j = 0; j < sorted_column_ids.size(); ++j) {
        tablet_chunk->get_column_raw_ptr_by_index(value_column_ids_by_order_with_orig_idx[j].second)
                ->append(*read_columns[j]);
    }
    return tablet_chunk;
}

StatusOr<ChunkPtr> ShortCircuitHybridScanNode::_scan_one_tablet(
        const TabletSharedPtr& tablet, const std::string& version, const std::vector<std::string>& value_field_names,
        const std::vector<starrocks::ColumnId>& value_column_ids, const Schema& value_schema) {
    // Rowid fetch needs either TabletUpdates (PK) or raw unmerged rows
    // (DUPLICATE). AGGREGATE/UNIQUE keys require merge-at-read semantics that
    // a raw rowid fetch cannot provide, so they keep the TableReader::scan
    // fallback.
    const KeysType keys_type = tablet->tablet_schema()->keys_type();
    if (tablet->updates() != nullptr || keys_type == KeysType::DUP_KEYS) {
        auto rowid_result = _scan_one_tablet_by_bitmap_rowids(tablet, version, value_column_ids, value_schema);
        if (rowid_result.ok()) {
            return std::move(rowid_result.value());
        }
        if (!rowid_result.status().is_not_supported()) {
            return rowid_result.status();
        }
        VLOG_ROW << "secondary short circuit: bitmap rowid path skipped on tablet " << tablet->tablet_id() << ": "
                 << rowid_result.status();
    }
    LocalTableReaderParams params;
    params.version = std::stol(version);
    params.tablet_id = tablet->get_tablet_info().tablet_id;
    auto table_reader = std::make_shared<TableReader>();
    RETURN_IF_ERROR(table_reader->init(params));
    ASSIGN_OR_RETURN(ChunkIteratorPtr iterator, table_reader->scan(value_field_names, _secondary_predicates));
    ChunkPtr tablet_chunk = ChunkFactory::new_chunk(value_schema, MAX_SECONDARY_SCAN_ROWS + 1);
    auto scan_chunk = ChunkFactory::new_chunk(value_schema, MAX_SECONDARY_SCAN_ROWS + 1);
    while (true) {
        scan_chunk->reset();
        Status status = iterator->get_next(scan_chunk.get());
        if (status.is_end_of_file()) {
            break;
        }
        RETURN_IF_ERROR(status);
        if (scan_chunk->num_rows() == 0) {
            break;
        }
        if (tablet_chunk->num_rows() + scan_chunk->num_rows() > MAX_SECONDARY_SCAN_ROWS) {
            return Status::InternalError(fmt::format("secondary short circuit matched more than {} rows, aborting",
                                                     MAX_SECONDARY_SCAN_ROWS));
        }
        tablet_chunk->append(*scan_chunk);
    }
    return tablet_chunk;
}

Status ShortCircuitHybridScanNode::_get_next_secondary(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_ERROR(_process_scan_chunk());
    auto result_chunk = RuntimeChunkHelper::new_chunk(*_tuple_desc, _value_chunk->num_rows());
    // scanned columns were requested in tuple slot order, so positional copy maps them
    for (size_t j = 0; j < _tuple_desc->slots().size(); ++j) {
        result_chunk->get_column_raw_ptr_by_slot_id(_tuple_desc->slots()[j]->id())
                ->append(*(_value_chunk->get_column_by_index(j).get()));
    }
    if (result_chunk->num_rows() > 0) {
        RETURN_IF_ERROR(ChunkPredicateEvaluator::eval_conjuncts(_conjunct_ctxs, result_chunk.get()));
    }
    *eos = true;
    *chunk = std::move(result_chunk);
    _num_rows_returned += (*chunk)->num_rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

// params: std::vector<int>& found
// found vector value
Status ShortCircuitHybridScanNode::_process_value_chunk(std::vector<bool>& found) {
    std::vector<string> value_field_names;
    vector<starrocks::ColumnId> value_column_ids;
    for (auto slot_desc : _tuple_desc->slots()) {
        auto field = _tablet_schema->schema()->get_field_by_name(slot_desc->col_name());
        if (field != nullptr && !field->is_key()) {
            value_field_names.emplace_back(field->name());
            value_column_ids.emplace_back(field->id());
        }
    }
    auto value_schema = std::make_unique<Schema>(_tablet_schema->schema(), value_column_ids);
    // tmp value_chunk, order not match key_chunk
    ChunkPtr value_chunk = ChunkFactory::new_chunk(*(value_schema), _num_rows);
    // final value_chunk, order match key_chunk
    _value_chunk = ChunkFactory::new_chunk(*(value_schema), _num_rows);

    std::vector<int> key_idx_to_value_idx(_num_rows, -1);
    int value_chunk_idx = 0;

    for (int i = 0; i < _tablets.size(); ++i) {
        LocalTableReaderParams params;
        params.version = std::stoi(_versions[i]);
        params.tablet_id = _tablets[i]->get_tablet_info().tablet_id;
        _table_reader = std::make_shared<TableReader>();
        RETURN_IF_ERROR(_table_reader->init(params));

        auto current_chunk = ChunkFactory::new_chunk(*(value_schema), _num_rows);
        // current tablet will return all key_chunk mapping whether has value
        // true , means vector idx of key_chunk have value
        std::vector<bool> curent_found;
        Status status =
                _table_reader->multi_get(*(_key_chunk.get()), value_field_names, curent_found, *(current_chunk.get()));
        if (!status.ok()) {
            // todo retry
            LOG(WARNING) << "fail to execute multi get: " << status.detailed_message();
            return status;
        }

        // merge all tablet result
        bool has_found_value = false;
        for (int key_idx = 0; key_idx < curent_found.size(); ++key_idx) {
            // 1 tablet will have many value hit predicate, so here need foreach end
            if (curent_found[key_idx] && key_idx_to_value_idx[key_idx] == -1) {
                // make sure found order is same between key_chunk and value_chunk
                key_idx_to_value_idx[key_idx] = value_chunk_idx;
                value_chunk_idx++;
                if (UNLIKELY(found[key_idx])) {
                    return Status::Corruption(
                            fmt::format("one key can't be found twice in short circuit, tablet_id: {}, key_idx: {}",
                                        params.tablet_id, key_idx));
                }
                found[key_idx] = true;
                has_found_value = true;
            }
        }
        if (has_found_value) {
            value_chunk->append(*(current_chunk.get()));
        }
    }

    // transform  value
    for (int key_idx = 0; key_idx < key_idx_to_value_idx.size(); ++key_idx) {
        if (key_idx_to_value_idx[key_idx] != -1) {
            _value_chunk->append(*(value_chunk.get()), key_idx_to_value_idx[key_idx], 1);
        }
    }

    return Status::OK();
}

} // namespace starrocks
