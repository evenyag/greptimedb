// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Empty input field"))]
    EmptyInputField {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing input field"))]
    MissingInputField {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Processor must be a map"))]
    ProcessorMustBeMap {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Processor {processor}: missing field: {field}"))]
    ProcessorMissingField {
        processor: String,
        field: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Processor {processor}: expect string value, but got {v:?}"))]
    ProcessorExpectString {
        processor: String,
        v: crate::etl::Value,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Processor {processor}: unsupported value {val}"))]
    ProcessorUnsupportedValue {
        processor: &'static str,
        val: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Processor key must be a string"))]
    ProcessorKeyMustBeString {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Processor {kind}: failed to parse {value}"))]
    ProcessorFailedToParseString {
        kind: String,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Processor must have a string key"))]
    ProcessorMustHaveStringKey {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported {processor} processor"))]
    UnsupportedProcessor {
        processor: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Field {field} must be a {ty}"))]
    FieldMustBeType {
        field: String,
        ty: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Field parse from string failed: {field}"))]
    FailedParseFieldFromString {
        #[snafu(source)]
        error: Box<dyn std::error::Error + Send + Sync>,
        field: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse {key} as int: {value}"))]
    FailedToParseIntKey {
        key: String,
        value: String,
        #[snafu(source)]
        error: std::num::ParseIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse {value} to int"))]
    FailedToParseInt {
        value: String,
        #[snafu(source)]
        error: std::num::ParseIntError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to parse {key} as float: {value}"))]
    FailedToParseFloatKey {
        key: String,
        value: String,
        #[snafu(source)]
        error: std::num::ParseFloatError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Processor {kind}: {key} not found in intermediate keys"))]
    IntermediateKeyIndex {
        kind: String,
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cmcd {k} missing value in {s}"))]
    CmcdMissingValue {
        k: String,
        s: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Part: {part} missing key in {s}"))]
    CmcdMissingKey {
        part: String,
        s: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Key must be a string, but got {k:?}"))]
    KeyMustBeString {
        k: yaml_rust::Yaml,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Csv read error"))]
    CsvRead {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: csv::Error,
    },
    #[snafu(display("Expected at least one record from csv format, but got none"))]
    CsvNoRecord {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Separator '{separator}' must be a single character, but got '{value}'"))]
    CsvSeparatorName {
        separator: &'static str,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Quote '{quote}' must be a single character, but got '{value}'"))]
    CsvQuoteName {
        quote: &'static str,
        value: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Parse date timezone error {value}"))]
    DateParseTimezone {
        value: String,
        #[snafu(source)]
        error: chrono_tz::ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Parse date error {value}"))]
    DateParse {
        value: String,
        #[snafu(source)]
        error: chrono::ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get local timezone"))]
    DateFailedToGetLocalTimezone {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get timestamp"))]
    DateFailedToGetTimestamp {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Processor {processor}: invalid format {s}"))]
    DateInvalidFormat {
        s: String,
        processor: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid Pattern: '{s}'. {detail}"))]
    DissectInvalidPattern {
        s: String,
        detail: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Empty pattern is not allowed"))]
    DissectEmptyPattern {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Split: '{split}' exceeds the input"))]
    DissectSplitExceedsInput {
        split: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Split: '{split}' does not match the input '{input}'"))]
    DissectSplitNotMatchInput {
        split: String,
        input: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Consecutive names are not allowed: '{name1}' '{name2}'"))]
    DissectConsecutiveNames {
        name1: String,
        name2: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("No matching pattern found"))]
    DissectNoMatchingPattern {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Modifier '{m}' already set, but found {modifier}"))]
    DissectModifierAlreadySet {
        m: String,
        modifier: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Append Order modifier is already set to '{n}', cannot be set to {order}"))]
    DissectAppendOrderAlreadySet {
        n: String,
        order: u32,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Order can only be set to Append Modifier, current modifier is {m}"))]
    DissectOrderOnlyAppend {
        m: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Order can only be set to Append Modifier"))]
    DissectOrderOnlyAppendModifier {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("End modifier already set: '{m}'"))]
    DissectEndModifierAlreadySet {
        m: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid resolution: {resolution}"))]
    EpochInvalidResolution {
        resolution: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Pattern is required"))]
    GsubPatternRequired {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Replacement is required"))]
    GsubReplacementRequired {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid regex pattern: {pattern}"))]
    Regex {
        #[snafu(source)]
        error: regex::Error,
        pattern: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Separator is required"))]
    JoinSeparatorRequired {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid method: {method}"))]
    LetterInvalidMethod {
        method: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("No named group found in regex {origin}"))]
    RegexNamedGroupNotFound {
        origin: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("No valid field found in {processor} processor"))]
    RegexNoValidField {
        processor: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("No valid pattern found in {processor} processor"))]
    RegexNoValidPattern {
        processor: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid method: {s}"))]
    UrlEncodingInvalidMethod {
        s: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Url decoding error"))]
    UrlEncodingDecode {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Invalid transform on_failure value: {value}"))]
    TransformOnFailureInvalidValue {
        value: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Transform element must be a map"))]
    TransformElementMustBeMap {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Transform {fields:?} type MUST BE set before default {default}"))]
    TransformTypeMustBeSet {
        fields: String,
        default: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Transform cannot be empty"))]
    TransformEmpty {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Column name must be unique, but got duplicated: {duplicates}"))]
    TransformColumnNameMustBeUnique {
        duplicates: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "Illegal to set multiple timestamp Index columns, please set only one: {columns}"
    ))]
    TransformMultipleTimestampIndex {
        columns: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Transform must have exactly one field specified as timestamp Index, but got {count}: {columns}"))]
    TransformTimestampIndexCount {
        count: usize,
        columns: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Null type not supported"))]
    CoerceUnsupportedNullType {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Null type not supported when to coerce '{ty}' type"))]
    CoerceUnsupportedNullTypeTo {
        ty: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Type: {ty} value not supported for Epoch"))]
    CoerceUnsupportedEpochType {
        ty: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to coerce string value '{s}' to type '{ty}'"))]
    CoerceStringToType {
        s: String,
        ty: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Can not coerce json type to {ty}"))]
    CoerceJsonTypeTo {
        ty: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "Can not coerce {ty} to json type. we only consider object and array to be json types."
    ))]
    CoerceTypeToJson {
        ty: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to coerce value: {msg}"))]
    CoerceIncompatibleTypes {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "Invalid resolution: '{resolution}'. Available resolutions: {valid_resolution}"
    ))]
    ValueInvalidResolution {
        resolution: String,
        valid_resolution: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse type: '{t}'"))]
    ValueParseType {
        t: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse {ty}: {v}"))]
    ValueParseInt {
        ty: String,
        v: String,
        #[snafu(source)]
        error: std::num::ParseIntError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse {ty}: {v}"))]
    ValueParseFloat {
        ty: String,
        v: String,
        #[snafu(source)]
        error: std::num::ParseFloatError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse {ty}: {v}"))]
    ValueParseBoolean {
        ty: String,
        v: String,
        #[snafu(source)]
        error: std::str::ParseBoolError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Default value not unsupported for type {value}"))]
    ValueDefaultValueUnsupported {
        value: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unsupported number type: {value}"))]
    ValueUnsupportedNumberType {
        value: serde_json::Number,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unsupported yaml type: {value:?}"))]
    ValueUnsupportedYamlType {
        value: yaml_rust::Yaml,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("key in Hash must be a string, but got {value:?}"))]
    ValueYamlKeyMustBeString {
        value: yaml_rust::Yaml,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Yaml load error."))]
    YamlLoad {
        #[snafu(source)]
        error: yaml_rust::ScanError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Prepare value must be an object"))]
    PrepareValueMustBeObject {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Column options error"))]
    ColumnOptions {
        #[snafu(source)]
        source: api::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unsupported index type: {value}"))]
    UnsupportedIndexType {
        value: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unsupported number type: {value:?}"))]
    UnsupportedNumberType {
        value: serde_json::Number,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Column datatype mismatch. For column: {column}, expected datatype: {expected}, actual datatype: {actual}"))]
    IdentifyPipelineColumnTypeMismatch {
        column: String,
        expected: String,
        actual: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        StatusCode::InvalidArguments
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
