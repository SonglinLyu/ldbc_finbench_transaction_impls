/**
 * Copyright 2022 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

#include <algorithm>
#include <exception>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include "lgraph/lgraph.h"
#include "lgraph/lgraph_edge_iterator.h"
#include "lgraph/lgraph_types.h"
#include "lgraph/lgraph_utils.h"
#include "lgraph/lgraph_result.h"
#include "tools/json.hpp"

using namespace lgraph_api;
using json = nlohmann::json;

template <class EIT>
class LabeledEdgeIterator : public EIT {
    uint16_t lid_;
    bool valid_;

   public:
    LabeledEdgeIterator(EIT&& eit, uint16_t lid) : EIT(std::move(eit)), lid_(lid) {
        valid_ = EIT::IsValid() && EIT::GetLabelId() == lid_;
    }

    bool IsValid() { return valid_; }

    bool Next() {
        if (!valid_) return false;
        valid_ = (EIT::Next() && EIT::GetLabelId() == lid_);
        return valid_;
    }

    void Reset(VertexIterator& vit, uint16_t lid) { Reset(vit.GetId(), lid); }

    void Reset(size_t vid, uint16_t lid, int64_t tid = 0) {
        lid_ = lid;
        if (std::is_same<EIT, OutEdgeIterator>::value) {
            EIT::Goto(EdgeUid(vid, 0, lid, tid, 0), true);
        } else {
            EIT::Goto(EdgeUid(0, vid, lid, tid, 0), true);
        }
        valid_ = (EIT::IsValid() && EIT::GetLabelId() == lid_);
    }
};

static LabeledEdgeIterator<OutEdgeIterator> LabeledOutEdgeIterator(VertexIterator& vit, uint16_t lid, int64_t tid = 0) {
    return LabeledEdgeIterator<OutEdgeIterator>(std::move(vit.GetOutEdgeIterator(EdgeUid(0, 0, lid, tid, 0), true)),
                                                lid);
}

static LabeledEdgeIterator<InEdgeIterator> LabeledInEdgeIterator(VertexIterator& vit, uint16_t lid, int64_t tid = 0) {
    return LabeledEdgeIterator<InEdgeIterator>(std::move(vit.GetInEdgeIterator(EdgeUid(0, 0, lid, tid, 0), true)), lid);
}

static LabeledEdgeIterator<OutEdgeIterator> LabeledOutEdgeIterator(Transaction& txn, int64_t vid, uint16_t lid,
                                                                   int64_t tid = 0) {
    return LabeledEdgeIterator<OutEdgeIterator>(std::move(txn.GetOutEdgeIterator(EdgeUid(vid, 0, lid, tid, 0), true)),
                                                lid);
}

static LabeledEdgeIterator<InEdgeIterator> LabeledInEdgeIterator(Transaction& txn, int64_t vid, uint16_t lid,
                                                                 int64_t tid = 0) {
    return LabeledEdgeIterator<InEdgeIterator>(std::move(txn.GetInEdgeIterator(EdgeUid(0, vid, lid, tid, 0), true)),
                                               lid);
}

extern "C" bool Process(GraphDB& db, const std::string& request, std::string& response) {
    static const std::string ACCOUNT_ID = "id";
    static const std::string ACCOUNT_LABEL = "Account";
    static const std::string TRANSFER_LABEL = "transfer";
    static const std::string TIMESTAMP = "timestamp";
    static const std::string AMOUNT = "amount";
    json output;
    int64_t id, start_time, end_time;
    int64_t limit = -1;
    try {
        json input = json::parse(request);
        parse_from_json(id, "id", input);
        parse_from_json(start_time, "startTime", input);
        parse_from_json(end_time, "endTime", input);
    } catch (std::exception& e) {
        output["msg"] = "json parse error: " + std::string(e.what());
        response = output.dump();
        return false;
    }

    auto txn = db.CreateReadTxn();

    // get the vertex iterator of source account
    auto account = txn.GetVertexByUniqueIndex(ACCOUNT_LABEL, ACCOUNT_ID, FieldData(id));
    auto vit = txn.GetVertexIterator();
    vit.Goto(account.GetId());

    // get the lid of edge "transfer"
    int16_t transfer_id = (int16_t)txn.GetEdgeLabelId(TRANSFER_LABEL);

    // go through all transfer-outs edges within (startTime, endTime)
    double sum_out = 0;
    double max_out = 0;
    int64_t count_out = 0;
    for (auto eit = LabeledOutEdgeIterator(vit, transfer_id, start_time);
            eit.IsValid(); eit.Next()) {
        auto timestamp = eit.GetField(TIMESTAMP).AsInt64();
        auto amount = eit.GetField(AMOUNT).AsDouble();
        if (timestamp == start_time) {
            continue;
        }
        if (timestamp >= end_time) {
            break;
        }
        count_out += 1;
        sum_out += amount;
        if (amount > max_out){
            max_out = amount;
        }
    }
    sum_out = std::round(sum_out * 1000.0) / 1000;
    max_out = std::round(max_out * 1000.0) / 1000;
    if (count_out == 0) max_out = -1;

    // go through all transfer-in edges within (startTime, endTime)
    double sum_in = 0;
    double max_in = 0;
    int64_t count_in = 0;
    for (auto eit = LabeledInEdgeIterator(vit, transfer_id, start_time);
            eit.IsValid(); eit.Next()) {
        auto timestamp = eit.GetField(TIMESTAMP).AsInt64();
        auto amount = eit.GetField(AMOUNT).AsDouble();
        if (timestamp == start_time) {
            continue;
        }
        if (timestamp >= end_time) {
            break;
        }
        count_in += 1;
        sum_in += amount;
        if (amount > max_in){
            max_in = amount;
        }
    }
    sum_in = std::round(sum_in * 1000.0) / 1000;
    max_in = std::round(max_in * 1000.0) / 1000;
    if (count_in == 0) max_in = -1;

    // add result
    std::vector<std::tuple<double, double, int64_t, double, double, int64_t>> result;
    result.emplace_back(sum_out, max_out, count_out, sum_in, max_in, count_in);
    
    lgraph_api::Result api_result(
        {{"sumEdge1Amount", LGraphType::DOUBLE}, {"maxEdge1Amount", LGraphType::DOUBLE}, {"numEdge1", LGraphType::INTEGER}, {"sumEdge2Amount", LGraphType::DOUBLE}, {"maxEdge2Amount", LGraphType::DOUBLE}, {"numEdge2", LGraphType::INTEGER}});
    for (auto& item : result) {
        auto r = api_result.MutableRecord();
        r->Insert("sumEdge1Amount", FieldData::Double(std::get<0>(item)));
        r->Insert("maxEdge1Amount", FieldData::Double(std::get<1>(item)));
        r->Insert("numEdge1", FieldData::Int64(std::get<2>(item)));
        r->Insert("sumEdge2Amount", FieldData::Double(std::get<3>(item)));
        r->Insert("maxEdge2Amount", FieldData::Double(std::get<4>(item)));
        r->Insert("numEdge2", FieldData::Int64(std::get<5>(item)));
    }
    response = api_result.Dump();
    return true;
}
