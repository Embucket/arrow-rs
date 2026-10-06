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

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet_variant::{Variant, VariantBuilder};
use parquet_variant_json::{JsonToVariant, VariantToJson, append_json};
use serde_json::Value;
use std::hint::black_box;

fn bench_from_json(c: &mut Criterion) {
    let large_array = format!(
        "[{}]",
        (0..1024)
            .map(|number| number.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let large_string = format!(r#"{{"payload":"{}","id":42}}"#, "x".repeat(8192));
    let duplicate_large = format!(r#"{{"a":"{}","a":0}}"#, "x".repeat(100_000));
    let inputs = [
        (
            "equal_integers",
            r#"{"id":123456789,"values":[1,2,3,4,5],"active":true}"#,
            true,
        ),
        (
            "decimal_mixed",
            r#"{"id":123456789,"name":"alice","values":[1,2.5,3e2,null],"active":true}"#,
            false,
        ),
        (
            "decimal_values",
            r#"{"small":1.23,"medium":999999999.0,"large":0.9999999999999999999}"#,
            false,
        ),
        (
            "equal_strings",
            r#"{"first":"unescaped alpha","second":"unescaped beta","third":"unescaped gamma"}"#,
            true,
        ),
        (
            "equal_escaped_strings",
            r#"{"first":"line one\nline two","second":"quote: \"value\"","third":"unicode: \u2764"}"#,
            true,
        ),
        (
            "decimal_nested",
            r#"{"outer":[{"id":1,"values":[1.25,2.50]},{"id":2,"values":[3.75,4.00]}]}"#,
            false,
        ),
        ("equal_large_array_1024", large_array.as_str(), true),
        ("equal_large_string_8k", large_string.as_str(), true),
        ("duplicate_large_100k", duplicate_large.as_str(), false),
    ];

    let mut group = c.benchmark_group("variant_from_json");
    for (name, json, equivalent_output) in inputs {
        if equivalent_output {
            let mut direct = VariantBuilder::new();
            direct.append_json(json).expect("valid JSON");
            let parsed: Value = serde_json::from_str(json).expect("valid JSON");
            let mut value_tree = VariantBuilder::new();
            append_json(&parsed, &mut value_tree).expect("valid Variant");
            let (direct_metadata, direct_value) = direct.finish();
            let (tree_metadata, tree_value) = value_tree.finish();
            let direct_json = Variant::try_new(&direct_metadata, &direct_value)
                .expect("valid direct Variant")
                .to_json_value()
                .expect("decodable direct Variant");
            let tree_json = Variant::try_new(&tree_metadata, &tree_value)
                .expect("valid tree Variant")
                .to_json_value()
                .expect("decodable tree Variant");
            assert_eq!(direct_json, tree_json, "{name}");
        }
        group.throughput(Throughput::Bytes(
            u64::try_from(json.len()).expect("input size"),
        ));
        group.bench_with_input(BenchmarkId::new("direct", name), json, |b, json| {
            b.iter(|| {
                let mut builder = VariantBuilder::new();
                builder.append_json(black_box(json)).expect("valid JSON");
                black_box(builder.finish())
            });
        });
        group.bench_with_input(BenchmarkId::new("value_tree", name), json, |b, json| {
            b.iter(|| {
                let value: Value = serde_json::from_str(black_box(json)).expect("valid JSON");
                let mut builder = VariantBuilder::new();
                append_json(&value, &mut builder).expect("valid Variant");
                black_box(builder.finish())
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_from_json);
criterion_main!(benches);
