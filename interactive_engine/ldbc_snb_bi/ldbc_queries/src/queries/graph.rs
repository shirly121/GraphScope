extern crate chrono;

use std::path::Path;
use std::sync::Arc;

use chrono::offset::{TimeZone, Utc};
use chrono::DateTime;
use graph_proxy::apis::GraphElement;
use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::prelude::*;
use mcsr::graph_db_impl::{CsrDB, SingleSubGraph, SubGraph};
use pegasus::configure_with_default;
use runtime::process::record::{Entry, Record};

use self::chrono::Datelike;

lazy_static! {
    pub static ref GRAPH: LargeGraphDB<DefaultId, InternalId> = _init_graph();
    pub static ref CSR: CsrDB<usize, usize> = _init_csr();
    pub static ref DATA_PATH: String = configure_with_default!(String, "DATA_PATH", "".to_string());
    pub static ref CSR_PATH: String = configure_with_default!(String, "CSR_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
    pub static ref COMMENT_REPLYOF_COMMENT_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(2, 5, 2, mcsr::graph::Direction::Incoming);
    pub static ref COMMENT_REPLYOF_POST_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(3, 5, 2, mcsr::graph::Direction::Incoming);
    pub static ref COMMENT_REPLYOF_COMMENT_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(2, 5, 2, mcsr::graph::Direction::Outgoing);
    pub static ref COMMENT_REPLYOF_POST_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(2, 5, 3, mcsr::graph::Direction::Outgoing);
    pub static ref COMMENT_HASTAG_TAG_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(7, 14, 2, mcsr::graph::Direction::Incoming);
    pub static ref POST_HASTAG_TAG_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(7, 14, 3, mcsr::graph::Direction::Incoming);
    pub static ref COMMENT_HASTAG_TAG_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(2, 14, 7, mcsr::graph::Direction::Outgoing);
    pub static ref POST_HASTAG_TAG_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(3, 14, 7, mcsr::graph::Direction::Outgoing);
    pub static ref FORUM_HASMODERATOR_PERSON_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 6, 4, mcsr::graph::Direction::Incoming);
    pub static ref FORUM_HASMODERATOR_PERSON_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(4, 6, 1, mcsr::graph::Direction::Outgoing);
    pub static ref FORUM_HASMEMBER_PERSON_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 12, 4, mcsr::graph::Direction::Incoming);
    pub static ref FORUM_HASMEMBER_PERSON_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(4, 12, 1, mcsr::graph::Direction::Outgoing);
    pub static ref FORUM_CONTAINEROF_POST_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(4, 10, 3, mcsr::graph::Direction::Outgoing);
    pub static ref FORUM_CONTAINEROF_POST_IN: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(3, 10, 4, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_ISLOCATEDIN_PLACE_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(0, 2, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_ISLOCATEDIN_PLACE_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(1, 2, 0, mcsr::graph::Direction::Outgoing);
    pub static ref COMMENT_HASCREATOR_PERSON_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(2, 0, 1, mcsr::graph::Direction::Outgoing);
    pub static ref POST_HASCREATOR_PERSON_OUT: SingleSubGraph<'static, usize, usize> =
        CSR.get_single_sub_graph(3, 0, 1, mcsr::graph::Direction::Outgoing);
    pub static ref COMMENT_HASCREATOR_PERSON_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 0, 2, mcsr::graph::Direction::Incoming);
    pub static ref POST_HASCREATOR_PERSON_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 0, 3, mcsr::graph::Direction::Incoming);
    pub static ref TAG_HASTYPE_TAGCLASS_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(6, 4, 7, mcsr::graph::Direction::Incoming);
    pub static ref PLACE_ISPARTOF_PLACE_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(0, 8, 0, mcsr::graph::Direction::Incoming);
    pub static ref PLACE_ISPARTOF_PLACE_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(0, 8, 0, mcsr::graph::Direction::Outgoing);
    pub static ref PERSON_LIKES_COMMENT_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(2, 7, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_LIKES_POST_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(3, 7, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_LIKES_COMMENT_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 7, 2, mcsr::graph::Direction::Outgoing);
    pub static ref PERSON_LIKES_POST_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 7, 3, mcsr::graph::Direction::Outgoing);
    pub static ref PERSON_HASINTEREST_TAG_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(7, 9, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_KNOWS_PERSON_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 1, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_KNOWS_PERSON_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 1, 1, mcsr::graph::Direction::Outgoing);
    pub static ref PERSON_WORKAT_ORGANISATION_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 11, 5, mcsr::graph::Direction::Outgoing);
    pub static ref PERSON_WORKAT_ORGANISATION_IN: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(5, 11, 1, mcsr::graph::Direction::Incoming);
    pub static ref PERSON_STUDYAT_ORGANISATION_OUT: SubGraph<'static, usize, usize> =
        CSR.get_sub_graph(1, 13, 5, mcsr::graph::Direction::Outgoing);
}

fn _init_graph() -> LargeGraphDB<DefaultId, InternalId> {
    println!("Read the graph data from {:?} for demo.", *DATA_PATH);
    GraphDBConfig::default()
        .root_dir(&(*DATA_PATH))
        .partition(*PARTITION_ID)
        .schema_file(
            &(DATA_PATH.as_ref() as &Path)
                .join(DIR_GRAPH_SCHEMA)
                .join(FILE_SCHEMA),
        )
        .open()
        .expect("Open graph error")
}

fn _init_csr() -> CsrDB<usize, usize> {
    // CsrDB::import(&*(CSR_PATH), *PARTITION_ID).unwrap()
    CsrDB::deserialize(&*(CSR_PATH), *PARTITION_ID).unwrap()
}

/// Typical symbols that split a string-format of a time data.
fn is_time_splitter(c: char) -> bool {
    c == '-' || c == ':' || c == ' ' || c == 'T' || c == 'Z' || c == '.'
}

pub fn parse_datetime(val: &str) -> GDBResult<u64> {
    let mut dt_str = val;
    #[allow(unused_assignments)]
    let mut s = String::new();
    let mut is_millis = false;
    if let Ok(millis) = dt_str.parse::<i64>() {
        if let Some(dt) = Utc.timestamp_millis_opt(millis).single() {
            if dt.year() > 1970 && dt.year() < 2030 {
                s = dt.to_rfc3339();
                dt_str = s.as_ref();
                is_millis = true;
            }
        }
    }
    let mut _time = String::with_capacity(dt_str.len());
    for c in dt_str.chars() {
        if c == '+' {
            // "2012-07-21T07:59:14.322+000", skip the content after "."
            break;
        } else if is_time_splitter(c) {
            continue; // replace any time splitter with void
        } else {
            _time.push(c);
        }
    }

    if is_millis {
        // pad '0' if not 'yyyyMMddHHmmssSSS'
        while _time.len() < 17 {
            // push the SSS to fill the datetime as the required format
            _time.push('0');
        }
    }
    Ok(_time.parse::<u64>()?)
}

pub fn global_id_to_id(global_id: u64, label_id: u8) -> u64 {
    global_id ^ ((label_id as u64) << 56)
}

pub fn date_to_timestamp(date: u64) -> u64 {
    if date > 1000000000 {
        let date_year = date / 10000000000000;
        let date_month = date / 100000000000 % 100;
        let date_day = date / 1000000000 % 100;
        let date_hour = date / 10000000 % 100;
        let date_min = date / 100000 % 100;
        let date_sec = date / 1000 % 100;
        let date_dt = DateTime::<Utc>::from_utc(
            chrono::NaiveDate::from_ymd_opt(date_year as i32, date_month as u32, date_day as u32)
                .expect("invalid or out-of-range date")
                .and_hms_opt(date_hour as u32, date_min as u32, date_sec as u32)
                .expect("invalid time"),
            Utc,
        );
        (date_dt.timestamp() * 1000) as u64
    } else {
        let date_year = date / 10000;
        let date_month = date / 100 % 100;
        let date_day = date % 100;
        let date_hour = 0;
        let date_min = 0;
        let date_sec = 0;
        let date_dt = DateTime::<Utc>::from_utc(
            chrono::NaiveDate::from_ymd_opt(date_year as i32, date_month as u32, date_day as u32)
                .expect("invalid or out-of-range date")
                .and_hms_opt(date_hour as u32, date_min as u32, date_sec as u32)
                .expect("invalid time"),
            Utc,
        );
        (date_dt.timestamp() * 1000) as u64
    }
}

pub fn get_partition(id: &u64, workers: usize, num_servers: usize) -> u64 {
    let id_usize = *id as usize;
    let magic_num = id_usize / num_servers;
    // The partitioning logics is as follows:
    // 1. `R = id - magic_num * num_servers = id % num_servers` routes a given id
    // to the machine R that holds its data.
    // 2. `R * workers` shifts the worker's id in the machine R.
    // 3. `magic_num % workers` then picks up one of the workers in the machine R
    // to do the computation.
    ((id_usize - magic_num * num_servers) * workers + magic_num % workers) as u64
}

pub(crate) fn get_entry_partition(entry: &Arc<Entry>, workers: usize, num_servers: usize) -> u64 {
    let v = entry.as_graph_vertex().unwrap();
    get_partition(&v.id(), workers as usize, num_servers)
}

pub(crate) fn get_record_curr_partition(r: &Record, workers: usize, num_servers: usize) -> u64 {
    let entry = r.get(None).unwrap();
    get_entry_partition(entry, workers, num_servers)
}

pub fn get_2d_partition(id_hi: u64, id_low: u64, workers: usize, num_servers: usize) -> u64 {
    let server_id = id_hi % num_servers as u64;
    let worker_id = id_low % workers as u64;
    server_id * workers as u64 + worker_id
}

pub fn get_csr_partition(id: &u64, workers: usize, num_servers: usize) -> u64 {
    let id_usize = *id as usize;
    let magic_num = id_usize / num_servers;
    // The partitioning logics is as follows:
    // 1. `R = id - magic_num * num_servers = id % num_servers` routes a given id
    // to the machine R that holds its data.
    // 2. `R * workers` shifts the worker's id in the machine R.
    // 3. `magic_num % workers` then picks up one of the workers in the machine R
    // to do the computation.
    ((id_usize - magic_num * num_servers) * workers + magic_num % workers) as u64
}
