use crate::config::RetryInterval;
use crate::error::LatteError;
use crate::scripting::bind;
use crate::scripting::cass_error::{CassError, CassErrorKind};
use crate::scripting::connect::ClusterInfo;
use crate::stats::session::SessionStats;
use bytes::Bytes;
use chrono::Utc;
use itertools::enumerate;
use rand::prelude::ThreadRng;
use rand::random;
use rune::runtime::{Object, Shared};
use rune::{Any, Value};
use scylla::prepared_statement::PreparedStatement;
use scylla::query::Query;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::error;
use try_lock::TryLock;

#[derive(Clone, Debug, PartialEq)]
pub struct PartitionGroup {
    pub n_rows_per_group: u64,
    pub n_partitions: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowDistribution {
    pub n_cycles: u64,
    pub n_rows_for_left: u64,
    pub n_rows_for_right: u64,
    pub n_rows_for_left_and_right: u64,
    pub n_rows_for_all_cycles: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RowDistributionPreset {
    pub total_rows: u64,
    pub partition_groups: Vec<PartitionGroup>,
    pub row_distributions: Vec<(RowDistribution, RowDistribution)>,
}

impl RowDistributionPreset {
    pub fn new(partition_groups: Vec<PartitionGroup>) -> RowDistributionPreset {
        let total_rows: u64 = partition_groups.iter().map(|pg| pg.n_rows_per_group).sum();
        RowDistributionPreset {
            total_rows,
            partition_groups,
            row_distributions: vec![],
        }
    }

    pub fn generate_row_distributions(&mut self) {
        let mut other_rows: u64 = self.total_rows;
        for partition_group in &self.partition_groups {
            // NOTE: Calculate the greatest common divisor allowing it to split it to 2 groups
            //       for getting better distribution results.
            //       This "greatest common divisioner" will be used as a number of distribution cycles
            //       based on the partition group proportions.
            other_rows -= partition_group.n_rows_per_group;
            let (cycles_num, (mult_n1, tail_n1), (mult_n2, tail_n2)) =
                max_gcd_with_tail(partition_group.n_rows_per_group, other_rows);
            let cycle_type_1 = (
                tail_n1 + tail_n2,
                mult_n1 + (tail_n1 > 0) as u64,
                mult_n2 + (tail_n2 > 0) as u64,
            );
            let cycle_type_2 = ((cycles_num - tail_n1 - tail_n2), mult_n1, mult_n2);
            self.row_distributions.push((
                RowDistribution {
                    n_cycles: cycle_type_1.0,
                    n_rows_for_left: cycle_type_1.1,
                    n_rows_for_right: cycle_type_1.2,
                    n_rows_for_left_and_right: cycle_type_1.1 + cycle_type_1.2,
                    n_rows_for_all_cycles: cycle_type_1.0 * cycle_type_1.1
                        + cycle_type_1.0 * cycle_type_1.2,
                },
                RowDistribution {
                    n_cycles: cycle_type_2.0,
                    n_rows_for_left: cycle_type_2.1,
                    n_rows_for_right: cycle_type_2.2,
                    n_rows_for_left_and_right: cycle_type_2.1 + cycle_type_2.2,
                    n_rows_for_all_cycles: cycle_type_2.0 * cycle_type_2.1
                        + cycle_type_2.0 * cycle_type_2.2,
                },
            ));
        }
    }

    pub async fn get_partition_idx(&self, idx: u64) -> u64 {
        self._get_partition_idx(
            idx % self.total_rows,
            0,
            self.partition_groups.clone(),
            self.row_distributions.clone(),
        )
        .await
    }

    async fn _get_partition_idx(
        &self,
        mut idx: u64,
        mut partn_offset: u64,
        partition_groups: Vec<PartitionGroup>,
        row_distributions: Vec<(RowDistribution, RowDistribution)>,
    ) -> u64 {
        if partition_groups.is_empty() {
            panic!("No partition groups found, cannot proceed");
        }
        if row_distributions.is_empty() {
            panic!("No row_distributions found, cannot proceed");
        }
        for (loop_i, current_partn) in enumerate(partition_groups) {
            let current_partn_count = current_partn.n_partitions;

            let current_row_distribution = row_distributions[loop_i].clone();
            let cycle_type_1 = current_row_distribution.0;
            let cycle_type_2 = current_row_distribution.1;

            let cycle_type_1_size = cycle_type_1.n_rows_for_left_and_right;
            let done_cycle_type_1_num: u64;
            let done_cycle_type_1_rows: u64;

            let cycle_type_2_size: u64;
            let mut done_cycle_type_2_num: u64 = 0;
            let done_cycle_type_2_rows: u64;

            if idx < cycle_type_1.n_rows_for_all_cycles {
                // NOTE: we must add shift equal to the size of right group to make it's idx
                //       be calculated correctly on the recursive call step.
                done_cycle_type_1_num = (idx + cycle_type_1.n_rows_for_right) / cycle_type_1_size;
                done_cycle_type_1_rows = done_cycle_type_1_num * cycle_type_1_size;
                if done_cycle_type_1_rows <= idx
                    && idx < cycle_type_1.n_rows_for_left + done_cycle_type_1_rows
                {
                    let ret = partn_offset
                        + (idx - done_cycle_type_1_rows
                            + done_cycle_type_1_num * cycle_type_1.n_rows_for_left)
                            % current_partn_count;
                    return ret;
                }
            } else {
                done_cycle_type_1_num = cycle_type_1.n_cycles;
                done_cycle_type_1_rows = done_cycle_type_1_num * cycle_type_1_size;

                cycle_type_2_size = cycle_type_2.n_rows_for_left_and_right;
                // NOTE: exclude cumulative size of all the cycles of the first type because it's number
                //       gets considered separately in other parts.
                //       Also, we must add shift equal to the size of the right group to make it's idx
                //       be calculated correctly on the recursive call step.
                done_cycle_type_2_num = (idx - done_cycle_type_1_rows
                    + cycle_type_2.n_rows_for_right)
                    / cycle_type_2_size;
                done_cycle_type_2_rows = done_cycle_type_2_num * cycle_type_2_size;

                let total_done_rows = done_cycle_type_1_rows + done_cycle_type_2_rows;
                if total_done_rows <= idx && idx < total_done_rows + cycle_type_2.n_rows_for_left {
                    let ret = partn_offset
                        + (idx
                            - done_cycle_type_1_num * cycle_type_1.n_rows_for_right
                            - done_cycle_type_2_rows
                            + done_cycle_type_2_num * cycle_type_2.n_rows_for_left)
                            % current_partn_count;
                    return ret;
                }
            }
            idx = idx
                - done_cycle_type_1_num * cycle_type_1.n_rows_for_left
                - done_cycle_type_2_num * cycle_type_2.n_rows_for_left;
            partn_offset += current_partn_count;
        }
        panic!(
            "Failed to match idx and partition idx! \
            Most probably row distribution values were incorrectly calculated \
            according to the partition groups data."
        );
    }
}

/// This is the main object that a workload script uses to interface with the outside world.
/// It also tracks query execution metrics such as number of requests, rows, response times etc.
#[derive(Any)]
pub struct Context {
    start_time: TryLock<Instant>,
    // NOTE: 'session' is defined as optional for being able to test methods
    // which don't 'depend on'/'use' the 'session' object.
    session: Option<Arc<scylla::Session>>,
    page_size: u64,
    statements: HashMap<String, Arc<PreparedStatement>>,
    stats: TryLock<SessionStats>,
    retry_number: u64,
    retry_interval: RetryInterval,
    partition_row_presets: HashMap<String, RowDistributionPreset>,
    #[rune(get, set, add_assign, copy)]
    pub load_cycle_count: u64,
    #[rune(get)]
    pub preferred_datacenter: String,
    #[rune(get)]
    pub data: Value,
    pub rng: ThreadRng,
}

// Needed, because Rune `Value` is !Send, as it may contain some internal pointers.
// Therefore, it is not safe to pass a `Value` to another thread by cloning it, because
// both objects could accidentally share some unprotected, `!Sync` data.
// To make it safe, the same `Context` is never used by more than one thread at once, and
// we make sure in `clone` to make a deep copy of the `data` field by serializing
// and deserializing it, so no pointers could get through.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub fn new(
        session: Option<scylla::Session>,
        page_size: u64,
        preferred_datacenter: String,
        retry_number: u64,
        retry_interval: RetryInterval,
    ) -> Context {
        Context {
            start_time: TryLock::new(Instant::now()),
            session: session.map(Arc::new),
            page_size,
            statements: HashMap::new(),
            stats: TryLock::new(SessionStats::new()),
            retry_number,
            retry_interval,
            partition_row_presets: HashMap::new(),
            load_cycle_count: 0,
            preferred_datacenter,
            data: Value::Object(Shared::new(Object::new()).unwrap()),
            rng: rand::thread_rng(),
        }
    }

    /// Clones the context for use by another thread.
    /// The new clone gets fresh statistics.
    /// The user data gets passed through serialization and deserialization to avoid
    /// accidental data sharing.
    pub fn clone(&self) -> Result<Self, LatteError> {
        let serialized = rmp_serde::to_vec(&self.data)?;
        let deserialized: Value = rmp_serde::from_slice(&serialized)?;
        Ok(Context {
            session: self.session.clone(),
            page_size: self.page_size,
            statements: self.statements.clone(),
            stats: TryLock::new(SessionStats::default()),
            retry_number: self.retry_number,
            retry_interval: self.retry_interval,
            partition_row_presets: self.partition_row_presets.clone(),
            load_cycle_count: self.load_cycle_count,
            preferred_datacenter: self.preferred_datacenter.clone(),
            data: deserialized,
            start_time: TryLock::new(*self.start_time.try_lock().unwrap()),
            rng: rand::thread_rng(),
        })
    }

    /// Returns cluster metadata such as cluster name and cassandra version.
    pub async fn cluster_info(&self) -> Result<Option<ClusterInfo>, CassError> {
        let cql = "SELECT cluster_name, release_version FROM system.local";

        match &self.session {
            Some(session) => {
                let rs = session
                    .query(cql, ())
                    .await
                    .map_err(|e| CassError::query_execution_error(cql, &[], e));
                match rs {
                    Ok(rs) => {
                        if let Some(rows) = rs.rows {
                            if let Some(row) = rows.into_iter().next() {
                                if let Ok((name, cassandra_version)) = row.into_typed() {
                                    return Ok(Some(ClusterInfo {
                                        name,
                                        cassandra_version,
                                    }));
                                }
                            }
                        }
                        Ok(None)
                    }
                    Err(e) => {
                        eprintln!("WARNING: {e}", e = e);
                        Ok(None)
                    }
                }
            }
            None => Err(CassError(CassErrorKind::Error(
                "'session' is not defined".to_string(),
            ))),
        }
    }

    /// Creates a preset for uneven row distribution among partitions
    #[allow(clippy::comparison_chain)]
    pub async fn init_partition_row_distribution_preset(
        &mut self,
        preset_name: &str,
        row_count: u64,
        rows_per_partitions_base: u64,
        mut rows_per_partitions_groups: &str, // "percent:base_multiplier, ..." -> "80:1,15:2,5:4"
    ) -> Result<(), CassError> {
        // Validate input data
        if preset_name.is_empty() {
            return Err(CassError(CassErrorKind::Error(
                "init_partition_row_distribution_preset: 'preset_name' cannot be empty".to_string(),
            )));
        }
        if row_count < 1 {
            return Err(CassError(CassErrorKind::Error(
                "init_partition_row_distribution_preset: 'row_count' cannot be less than 1"
                    .to_string(),
            )));
        }
        if rows_per_partitions_base < 1 {
            return Err(CassError(CassErrorKind::Error(
                "init_partition_row_distribution_preset: 'rows_per_partitions_base' cannot be less than 1".to_string()
            )));
        }

        // Parse the 'rows_per_partitions_groups' string parameter into a HashMap
        let mut partn_multipliers: HashMap<String, (f64, f64)> = HashMap::new();
        if rows_per_partitions_groups.is_empty() {
            rows_per_partitions_groups = "100:1";
        }
        let mut summary_percentage: f64 = 0.0;
        let mut duplicates_dump: Vec<String> = Vec::new();
        for pair in rows_per_partitions_groups.split(',') {
            let processed_pair = &pair.replace(" ", "");
            if duplicates_dump.contains(processed_pair) {
                return Err(CassError(CassErrorKind::Error(format!(
                    "init_partition_row_distribution_preset: found duplicates pairs - '{processed_pair}'")
                )));
            }
            let parts: Vec<&str> = processed_pair.split(':').collect();
            if let (Some(key), Some(value)) = (parts.first(), parts.get(1)) {
                if let (Ok(k), Ok(v)) = (key.parse::<f64>(), value.parse::<f64>()) {
                    let current_pair_key = format!("{k}:{v}");
                    partn_multipliers.insert(current_pair_key.clone(), (k, v));
                    summary_percentage += k;
                    duplicates_dump.push(current_pair_key);
                } else {
                    return Err(CassError(CassErrorKind::Error(format!(
                        "init_partition_row_distribution_preset: \
                        Wrong sub-value provided in the 'rows_per_partitions_groups' parameter: '{processed_pair}'. \
                        It must be set of integer pairs separated with a ':' symbol. Example: '49.1:1,49:2,1.9:2.5'")
                    )));
                }
            }
        }
        if (summary_percentage - 100.0).abs() > 0.01 {
            return Err(CassError(CassErrorKind::Error(format!(
                "init_partition_row_distribution_preset: \
                summary of partition percentage must be '100'. Got '{summary_percentage}' instead"
            ))));
        }

        // Calculate values
        let mut partn_sizes: HashMap<String, (f64, u64)> = HashMap::new();
        let mut partn_counts: HashMap<String, (f64, u64)> = HashMap::new();
        let mut partn_cycle_size: f64 = 0.0;
        for (key, (partn_percent, partn_multiplier)) in &partn_multipliers {
            partn_sizes.insert(
                key.to_string(),
                (
                    *partn_percent,
                    ((rows_per_partitions_base as f64) * partn_multiplier) as u64,
                ),
            );
            let partition_type_size: f64 =
                rows_per_partitions_base as f64 * partn_multiplier * partn_percent / 100.0;
            partn_cycle_size += partition_type_size;
        }
        let mut partn_count: u64 = (row_count as f64 / partn_cycle_size) as u64;
        for (key, (partn_percent, _partn_multiplier)) in &partn_multipliers {
            let current_partn_count: u64 = ((partn_count as f64) * partn_percent / 100.0) as u64;
            partn_counts.insert(key.to_string(), (*partn_percent, current_partn_count));
        }
        partn_count = partn_counts.values().map(|&(_, last)| last).sum();

        // Combine calculated data into a vector of tuples
        let mut actual_row_count: u64 = 0;
        let mut partitions: Vec<(f64, u64, u64, f64)> = Vec::new();
        for (key, (_partn_percent, partn_cnt)) in &partn_counts {
            if let Some((_partn_percent, partn_size)) = partn_sizes.get(key) {
                if let Some((partn_percent, partn_multiplier)) = partn_multipliers.get(key) {
                    partitions.push((*partn_percent, *partn_cnt, *partn_size, *partn_multiplier));
                    actual_row_count += partn_cnt * partn_size;
                }
            }
        }
        partitions.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));

        // Adjust partitions based on the difference between requested and total row count
        let mut row_count_diff: u64 = 0;
        if row_count > actual_row_count {
            row_count_diff = row_count - actual_row_count;
            let smallest_partn_count_diff = row_count_diff / partitions[0].2;
            if smallest_partn_count_diff > 0 {
                partn_count += smallest_partn_count_diff;
                partitions[0].1 += smallest_partn_count_diff;
                let additional_rows: u64 = smallest_partn_count_diff * partitions[0].2;
                actual_row_count += additional_rows;
                row_count_diff -= additional_rows;
            }
        } else if row_count < actual_row_count {
            row_count_diff = actual_row_count - row_count;
            let mut smallest_partn_count_diff = row_count_diff / partitions[0].2;
            if row_count_diff % partitions[0].2 > 0 {
                smallest_partn_count_diff += 1;
            }
            if smallest_partn_count_diff > 0 {
                partn_count -= smallest_partn_count_diff;
                partitions[0].1 -= smallest_partn_count_diff;
                actual_row_count -= smallest_partn_count_diff * partitions[0].2;
                let additional_rows: u64 = smallest_partn_count_diff * partitions[0].2;
                actual_row_count -= additional_rows;
                row_count_diff = additional_rows - row_count_diff;
            }
        }
        if row_count_diff > 0 {
            partn_count += 1;
            let mut same_size_exists = false;
            for (i, partition) in enumerate(partitions.clone()) {
                if partition.2 == row_count_diff {
                    partitions[i].1 += 1;
                    same_size_exists = true;
                    break;
                }
            }
            if !same_size_exists {
                partitions.push((
                    (100000.0 / (partn_count as f64)).round() / 1000.0,
                    1,
                    row_count_diff,
                    1.0,
                ));
            }
            actual_row_count += row_count_diff;
        }
        partitions.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));

        // Print calculated values
        let partitions_str = partitions
            .iter()
            .map(|(_percent, partns, rows, _multiplier)| {
                let percent = *partns as f64 / partn_count as f64 * 100.0;
                let percent_str = format!("{:.10}", percent);
                let parts = percent_str.split('.').collect::<Vec<_>>();
                if parts.len() == 2 {
                    let int_part = parts[0];
                    let mut frac_part: String = "".to_string();
                    if parts[1].matches("0").count() != parts[1].len() {
                        frac_part = parts[1]
                            .chars()
                            .take_while(|&ch| ch == '0')
                            .chain(parts[1].chars().filter(|&ch| ch != '0').take(2))
                            .collect::<String>();
                    }
                    if !frac_part.is_empty() {
                        frac_part = format!(".{}", frac_part);
                    }
                    format!("{}(~{}{}%):{}", partns, int_part, frac_part, rows)
                } else {
                    format!("{}(~{}%):{}", partns, parts[0], rows)
                }
            })
            .collect::<Vec<String>>()
            .join(", ");
        println!(
            "info: init_partition_row_distribution_preset: \
             preset_name={preset_name}, total_partitions={partn_count}, total_rows={total_rows}\
            , partitions/rows -> {partitions}",
            preset_name = preset_name,
            partn_count = partn_count,
            total_rows = actual_row_count,
            partitions = partitions_str,
        );

        // Save data for further usage
        let mut partition_groups = vec![];
        for partition in partitions {
            if partition.1 > 0 {
                partition_groups.push(PartitionGroup {
                    n_rows_per_group: partition.1 * partition.2,
                    n_partitions: partition.1,
                });
            }
        }
        // NOTE: sort partition groups in the size descending order to minimize the cumulative
        // computation cost for determining the stress_idx-partition_idx relations.
        partition_groups.sort_by(|a, b| (b.n_rows_per_group).cmp(&(a.n_rows_per_group)));
        let mut row_distribution_preset = RowDistributionPreset::new(partition_groups);
        // NOTE: generate row distributions only after the partition groups are finished with changes
        row_distribution_preset.generate_row_distributions();
        self.partition_row_presets
            .insert(preset_name.to_string(), row_distribution_preset);

        Ok(())
    }

    /// Returns a partition index based on the stress operation index and a preset of values
    pub async fn get_partition_idx(&self, preset_name: &str, idx: u64) -> Result<u64, CassError> {
        let preset = self.partition_row_presets.get(preset_name).ok_or_else(|| {
            CassError(CassErrorKind::PartitionRowPresetNotFound(
                preset_name.to_string(),
            ))
        })?;
        Ok(preset.get_partition_idx(idx).await)
    }

    /// Returns list of datacenters used by nodes
    pub async fn get_datacenters(&self) -> Result<Vec<String>, CassError> {
        match &self.session {
            Some(session) => {
                let dc_info = session.get_cluster_data().get_datacenters_info();
                let mut datacenters: Vec<String> = dc_info.keys().cloned().collect();
                datacenters.sort();
                Ok(datacenters)
            }
            None => Err(CassError(CassErrorKind::Error(
                "'session' is not defined".to_string(),
            ))),
        }
    }

    /// Prepares a statement and stores it in an internal statement map for future use.
    pub async fn prepare(&mut self, key: &str, cql: &str) -> Result<(), CassError> {
        match &self.session {
            Some(session) => {
                let query = Query::new(cql).with_page_size(self.page_size as i32);
                let statement = session
                    .prepare(query)
                    .await
                    .map_err(|e| CassError::prepare_error(cql, e))?;
                self.statements.insert(key.to_string(), Arc::new(statement));
                Ok(())
            }
            None => Err(CassError(CassErrorKind::Error(
                "'session' is not defined".to_string(),
            ))),
        }
    }

    /// Executes an ad-hoc CQL statement with no parameters. Does not prepare.
    pub async fn execute(&self, cql: &str) -> Result<(), CassError> {
        match &self.session {
            Some(session) => {
                let query = Query::new(cql).with_page_size(self.page_size as i32);
                let mut all_pages_duration = Duration::ZERO;
                let mut paging_state: Option<Bytes> = None;
                let mut rows_cnt: u64 = 0;

                let mut current_attempt_num = 0;
                while current_attempt_num <= self.retry_number {
                    let start_time = self.stats.try_lock().unwrap().start_request();
                    let rs = session
                        .query_paged(query.clone(), (), paging_state.clone())
                        .await;
                    let current_duration = Instant::now() - start_time;
                    match rs {
                        Ok(ref page) => {
                            paging_state = page.paging_state.clone();
                            rows_cnt += page.rows.as_ref().map(|r| r.len()).unwrap_or(0) as u64;
                            all_pages_duration += current_duration;
                        }
                        Err(e) => {
                            let current_error =
                                CassError::query_execution_error(cql, &[], e.clone());
                            handle_retry_error(self, current_attempt_num, current_error).await;
                            current_attempt_num += 1;
                            continue;
                        }
                    }
                    if paging_state.is_some() {
                        current_attempt_num = 0;
                        continue; // get next page
                    }
                    self.stats.try_lock().unwrap().complete_request(
                        all_pages_duration,
                        Some(rows_cnt),
                        &rs,
                    );
                    rs.map_err(|e| CassError::query_execution_error(cql, &[], e.clone()))?;
                    return Ok(());
                }
                Err(CassError::query_retries_exceeded(self.retry_number))
            }
            None => Err(CassError(CassErrorKind::Error(
                "'session' is not defined".to_string(),
            ))),
        }
    }

    /// Executes a statement prepared and registered earlier by a call to `prepare`.
    pub async fn execute_prepared(&self, key: &str, params: Value) -> Result<(), CassError> {
        let statement = self
            .statements
            .get(key)
            .ok_or_else(|| CassError(CassErrorKind::PreparedStatementNotFound(key.to_string())))?;

        let params = bind::to_scylla_query_params(&params, statement.get_variable_col_specs())?;
        match &self.session {
            Some(session) => {
                let mut all_pages_duration = Duration::ZERO;
                let mut paging_state: Option<Bytes> = None;
                let mut rows_cnt: u64 = 0;

                let mut current_attempt_num = 0;
                while current_attempt_num <= self.retry_number {
                    let start_time = self.stats.try_lock().unwrap().start_request();
                    let rs = session
                        .execute_paged(statement, params.clone(), paging_state.clone())
                        .await;
                    let current_duration = Instant::now() - start_time;
                    match rs {
                        Ok(ref page) => {
                            paging_state = page.paging_state.clone();
                            rows_cnt += page.rows.as_ref().map(|r| r.len()).unwrap_or(0) as u64;
                            all_pages_duration += current_duration;
                        }
                        Err(e) => {
                            let current_error = CassError::query_execution_error(
                                statement.get_statement(),
                                &params,
                                e.clone(),
                            );
                            handle_retry_error(self, current_attempt_num, current_error).await;
                            current_attempt_num += 1;
                            continue;
                        }
                    }
                    if paging_state.is_some() {
                        current_attempt_num = 0;
                        continue; // get next page
                    }
                    self.stats.try_lock().unwrap().complete_request(
                        all_pages_duration,
                        Some(rows_cnt),
                        &rs,
                    );
                    rs.map_err(|e| {
                        CassError::query_execution_error(statement.get_statement(), &params, e)
                    })?;
                    return Ok(());
                }
                Err(CassError::query_retries_exceeded(self.retry_number))
            }
            None => Err(CassError(CassErrorKind::Error(
                "'session' is not defined".to_string(),
            ))),
        }
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.start_time.try_lock().unwrap().elapsed().as_secs_f64()
    }

    /// Returns the current accumulated request stats snapshot and resets the stats.
    pub fn take_session_stats(&self) -> SessionStats {
        let mut stats = self.stats.try_lock().unwrap();
        let result = stats.clone();
        stats.reset();
        result
    }

    /// Resets query and request counters
    pub fn reset(&self) {
        self.stats.try_lock().unwrap().reset();
        *self.start_time.try_lock().unwrap() = Instant::now();
    }
}

pub fn get_exponential_retry_interval(
    min_interval: Duration,
    max_interval: Duration,
    current_attempt_num: u64,
) -> Duration {
    let min_interval_float: f64 = min_interval.as_secs_f64();
    let mut current_interval: f64 =
        min_interval_float * (2u64.pow(current_attempt_num.try_into().unwrap_or(0)) as f64);

    // Add jitter
    current_interval += random::<f64>() * min_interval_float;
    current_interval -= min_interval_float / 2.0;

    Duration::from_secs_f64(current_interval.min(max_interval.as_secs_f64()))
}

pub async fn handle_retry_error(
    ctxt: &Context,
    current_attempt_num: u64,
    current_error: CassError,
) {
    let current_retry_interval = get_exponential_retry_interval(
        ctxt.retry_interval.min,
        ctxt.retry_interval.max,
        current_attempt_num,
    );

    let mut next_attempt_str = String::new();
    let is_last_attempt = current_attempt_num == ctxt.retry_number;
    if !is_last_attempt {
        next_attempt_str += &format!("[Retry in {} ms]", current_retry_interval.as_millis());
    }
    let err_msg = format!(
        "{}: [ERROR][Attempt {}/{}]{} {}",
        Utc::now().format("%Y-%m-%d %H:%M:%S%.3f"),
        current_attempt_num,
        ctxt.retry_number,
        next_attempt_str,
        current_error,
    );
    error!("{}", err_msg);
    if !is_last_attempt {
        ctxt.stats.try_lock().unwrap().store_retry_error(err_msg);
        tokio::time::sleep(current_retry_interval).await;
    } else {
        eprintln!("{}", err_msg);
    }
}

/// Computes the greatest common divisor of 2 numbers, useful for rows distribution among DB partitions
fn gcd(n1: u64, n2: u64) -> u64 {
    if n2 == 0 {
        n1
    } else {
        gcd(n2, n1 % n2)
    }
}

/// Takes numbers of rows for 2 DB partition groups and calculates the best approach
///   for getting the most dispered and the least clustered, by partition sizes, distribution.
#[rustfmt::skip]
fn max_gcd_with_tail(n1: u64, n2: u64) -> (
    u64,             // greatest common divisor
    (u64, u64),      // (multiplier_based_on_n1, tail_n1)
    (u64, u64),      // (multiplier_based_on_n2, tail_n2)
) {
    let mut max_gcd = 0;
    let mut best_split_n1 = (0, 0);
    let mut best_split_n2 = (0, 0);

    // NOTE: allow to vary number by 1 percent of it's size for extending chances to bigger common divisor
    // That 'tail'/'diff' which is taken out of rows number for computing greatest common divisor
    // later will be used in one of two cycle types utilized for distribution of rows among DB partitions.
    let max_tail_n1 = n1 / 100;
    // Try to split 'n1'
    for tail_n1 in 0..=max_tail_n1 {
        let head_n1 = n1 - tail_n1;
        let gcd_value = gcd(head_n1, n2);
        if gcd_value > max_gcd {
            max_gcd = gcd_value;
            best_split_n1 = ((head_n1 / gcd_value), tail_n1);
            best_split_n2 = ((n2 / gcd_value), 0);
        }
    }

    let max_tail_n2 = n2 / 100;
    // Try to split 'n2'
    for tail_n2 in 0..=max_tail_n2 {
        let head_n2 = n2 - tail_n2;
        let gcd_value = gcd(n1, head_n2);
        if gcd_value > max_gcd {
            max_gcd = gcd_value;
            best_split_n1 = ((n1 / gcd_value), 0);
            best_split_n2 = ((head_n2 / gcd_value), tail_n2);
        }
    }

    (max_gcd, best_split_n1, best_split_n2)
}

#[rustfmt::skip]
#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: if tests which use session object get added
    // then need to define the 'SCYLLA_URI="172.17.0.2:9042"' env var
    // and create a DB session like following:
    //     let session = tokio::runtime::Runtime::new()
    //         .unwrap()
    //         .block_on(async {
    //             let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    //             SessionBuilder::new().known_node(uri).build().await.unwrap()
    //         });
    //      let mut ctxt: Context = Context::new(Some(session), ...);

    fn init_and_use_partition_row_distribution_preset(
        row_count: u64,
        rows_per_partitions_base_and_groups_mapping: Vec<(u64, String)>,
        expected_partition_groups: Vec<PartitionGroup>,
        expected_idx_partition_idx_mapping: Vec<(u64, u64)>,
    ) {
        for (rows_per_partitions_base, rows_per_partitions_groups) in rows_per_partitions_base_and_groups_mapping {
            let mut ctxt: Context = Context::new(
                None, 501, "foo-dc".to_string(), 0, RetryInterval::new("1,2").expect("REASON"),
            );
            let preset_name = "foo_name";

            assert!(ctxt.partition_row_presets.is_empty(), "The 'partition_row_presets' HashMap should not be empty");

            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let _ = ctxt.init_partition_row_distribution_preset(
                    preset_name, row_count, rows_per_partitions_base, &rows_per_partitions_groups).await;
            });

            assert!(!ctxt.partition_row_presets.is_empty(), "The 'partition_row_presets' HashMap should not be empty");
            let actual_preset = ctxt.partition_row_presets.get(preset_name)
                .unwrap_or_else(|| panic!("Preset with name '{}' was not found", preset_name));
            assert_eq!(expected_partition_groups, actual_preset.partition_groups);

            for (idx, expected_partition_idx) in expected_idx_partition_idx_mapping.clone() {
                let partition_idx = tokio::runtime::Runtime::new().unwrap().block_on(async {
                    ctxt.get_partition_idx(preset_name, idx).await
                }).expect("Failed to get partition index");
                assert_eq!(
                    expected_partition_idx, partition_idx, "{}",
                    format_args!(
                        "Using '{}' idx expected partition_idx is '{}', but got '{}'",
                        idx, expected_partition_idx, partition_idx
                    )
                );
            }
        }
    }

    #[test]
    fn test_partition_row_distribution_preset_01_pos_single_group_evenly_divisible() {
        // total_partitions=40, total_rows=1000, partitions/rows -> 40(~100%):25
        init_and_use_partition_row_distribution_preset(
            1000,
            vec![(25, "100:1".to_string())],
            vec![PartitionGroup{ n_rows_per_group: 1000, n_partitions: 40}],
            vec![
                (0, 0), (1, 1), (2, 2), (39, 39), (40, 0), (41, 1), (42, 2), (999, 39),
                (1000, 0), (1001, 1), (1039, 39), (1040, 0), (1999, 39),
                (2000, 0), (2001, 1), (2039, 39), (2040, 0), (2999, 39),
            ],
        );
    }

    #[test]
    fn test_partition_row_distribution_preset_02_pos_single_group_unevenly_divisible() {
        // total_partitions=77, total_rows=1000, partitions/rows -> 76(~98.71%):13, 1(~1.29%):12
        init_and_use_partition_row_distribution_preset(
            1000,
            vec![(13, "100:1".to_string())],
            vec![
                PartitionGroup{ n_rows_per_group: 988, n_partitions: 76},
                PartitionGroup{ n_rows_per_group: 12, n_partitions: 1},
            ],
            vec![
                // 'stress_idx/rows_count' < 1
                // 4 cycles 83+1
                (0, 0),    (75, 75),  (76, 0),   (77, 1),  (82, 6),   (83, 76),
                (84, 7),   (85, 8),   (152, 75), (153, 0), (166, 13), (167, 76),
                (168, 14), (169, 15), (229, 75), (230, 0), (250, 20), (251, 76),
                (252, 21), (253, 22), (306, 75), (307, 0), (334, 27), (335, 76),
                // 8 cycles 82+1
                (336, 28), (337, 29), (383, 75), (384, 0), (417, 33), (418, 76),
                (419, 34), (420, 35), (460, 75), (461, 0), (500, 39), (501, 76),
                (502, 40), (503, 41), (537, 75), (538, 0), (583, 45), (584, 76),
                (585, 46), (586, 47), (614, 75), (615, 0), (666, 51), (667, 76),
                (668, 52), (669, 53), (691, 75), (692, 0), (749, 57), (750, 76),
                (751, 58), (752, 59), (768, 75), (769, 0), (832, 63), (833, 76),
                (834, 64), (835, 65), (845, 75), (846, 0), (915, 69), (916, 76),
                (917, 70), (918, 71), (922, 75), (923, 0), (998, 75), (999, 76),

                // 1 <= 'stress_idx/rows_count' < 2
                (1000, 0),  (1075, 75), (1076, 0),  (1077, 1), (1082, 6),  (1083, 76),
                (1917, 70), (1918, 71), (1922, 75), (1923, 0), (1998, 75), (1999, 76),

                // 2 <= 'stress_idx/rows_count' < 3
                (2000, 0),  (2075, 75), (2076, 0),  (2077, 1), (2082, 6),  (2083, 76),
                (2917, 70), (2918, 71), (2922, 75), (2923, 0), (2998, 75), (2999, 76),
            ],
        );
    }

    #[test]
    fn test_partition_row_distribution_preset_03_pos_multiple_groups_with_implicit_one() {
        // total_partitions=90, total_rows=1000,
        //   partitions/rows -> 46(~51.11%):6, 26(~28.88%):12, 17(~18.88%):24, 1(~1.11%):4
        init_and_use_partition_row_distribution_preset(
            1000,
            vec![
                (6, "50:1,30:2,20:4".to_string()),
                (12, "50:0.5,30:1,20:2".to_string()),
                (24, "50:0.25,30:0.5,20:1".to_string()),
            ],
            vec![
                PartitionGroup{ n_rows_per_group: 408, n_partitions: 17},
                PartitionGroup{ n_rows_per_group: 312, n_partitions: 26},
                PartitionGroup{ n_rows_per_group: 276, n_partitions: 46},
                PartitionGroup{ n_rows_per_group: 4, n_partitions: 1},
            ],
            vec![
                // 1) Partitions 0-16, 24 rows each. 1 cycle of 12+16, then 16 cycles of 11+16
                //    0-11, 28-38, 55-65, 82-92, 109-119, 136-146, 163-173,
                //    ..., 190-200, 217-227, 244-254, 271-281, ...
                (0, 0), (1, 1), (11, 11),
                (28, 12), (32, 16), (33, 0), (34, 1), (38, 5),
                (55, 6), (56, 7), (65, 16),
                (82, 0), (83, 1), (92, 10),
                (109, 11), (114, 16), (115, 0), (119, 4),
                (136, 5), (146, 15),
                (163, 16), (164, 0), (165, 1), (173, 9),

                // 2) Partitions 17-42, 12 rows each. 2 cycles of 32+28 then 8 cycles of 31+28
                //    12-27, 39-54, 105-108, 120-135, 147-158, 209-216, 228-243, 255-261, ...
                (12, 17), (27, 32),
                (39, 33), (48, 42), (49, 17), (54, 22),
                (105, 23), (108, 26),
                (120, 27), (135, 42),
                (147, 17), (158, 28),
                (209, 29), (216, 36),
                (228, 37), (233, 42), (234, 17), (243, 26),
                (255, 27), (261, 33),

                // 3) Partitions 43-88 , 6 rows each. 4 cycles 69+1
                // 66-81, 93-104, 159-162, 174-189, 201-208, 262-270, 282-285, 287-297, ...
                (66, 43), (81, 58),
                (93, 59), (104, 70),
                (159, 71), (162, 74),
                (174, 75), (187, 88), (188, 43), (189, 44),
                (201, 45), (208, 52),
                (262, 53), (270, 61),
                (282, 62), (285, 65), (287, 66), (297, 76),

                // 4) Partition 89, 4 rows.
                (286, 89), (506, 89), (779, 89), (999, 89),
            ],
        );
    }

    #[test]
    fn test_partition_row_distribution_preset_04_pos_multiple_groups_without_implicit_one() {
        // total_partitions=664, total_rows=10000,
        //   partitions/rows -> 332(~50%):20, 331(~49.84%):10, 1(~0.15%):50
        init_and_use_partition_row_distribution_preset(
            10000,
            vec![(10, "49.9:1,49.9:2, 0.2:5".to_string())],
            vec![
                PartitionGroup{ n_rows_per_group: 6640, n_partitions: 332},
                PartitionGroup{ n_rows_per_group: 3310, n_partitions: 331},
                PartitionGroup{ n_rows_per_group: 50, n_partitions: 1},
            ],
            vec![
                // 1) Partitions 0-331, 20 rows each. 60 cycles of 48+24 then 80 cycles of 47:24
                (0, 0), (47, 47),
                (72, 48), (119, 95),
                (144, 96), (191, 143),
                (216, 144), (263, 191),

                // 2) Partitions 332-662, 10 rows each. 10 cycles of 67+1 then 40 cycles of 66+1
                (48, 332), (71, 355),
                (120, 356), (143, 379),
                (192, 380), (210, 398),
                (212, 399), (215, 402),

                // 3) Partition 663. 50 rows.
                (211, 663), (9999, 663),

                // Repetition
                (10000, 0), (19999, 663),
            ],
        );
    }

    #[test]
    fn test_partition_row_distribution_preset_05_pos_multiple_presets() {
        let name_foo: String = "foo".to_string();
        let name_bar: String = "bar".to_string();
        let mut ctxt: Context = Context::new(
            None, 501, "foo-dc".to_string(), 0, RetryInterval::new("1,2").expect("REASON"),
        );

        assert!(ctxt.partition_row_presets.is_empty(), "The 'partition_row_presets' HashMap should be empty");
        let foo_value = ctxt.partition_row_presets.get(&name_foo);
        assert_eq!(None, foo_value);

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            ctxt.init_partition_row_distribution_preset(
                &name_foo, 1000, 10, "100:1").await
        }).unwrap_or_else(|_| panic!("The '{}' preset must have been created successfully", name_foo));
        assert!(!ctxt.partition_row_presets.is_empty(), "The 'partition_row_presets' HashMap should not be empty");
        ctxt.partition_row_presets.get(&name_foo)
            .unwrap_or_else(|| panic!("Preset with name '{}' was not found", name_foo));

        let absent_bar = ctxt.partition_row_presets.get(&name_bar);
        assert_eq!(None, absent_bar, "{}", format_args!("The '{}' preset was expected to be absent", name_bar));

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            ctxt.init_partition_row_distribution_preset(
                &name_bar, 1000, 10, "90:1,10:2").await
        }).unwrap_or_else(|_| panic!("The '{}' preset must have been created successfully", name_bar));
        ctxt.partition_row_presets.get(&name_bar)
            .unwrap_or_else(|| panic!("Preset with name '{}' was not found", name_bar));
    }

    fn false_input_for_partition_row_distribution_preset(
        preset_name: String,
        row_count: u64,
        rows_per_partitions_base: u64,
        rows_per_partitions_groups: String,
    ) {
        let mut ctxt: Context = Context::new(
            None, 501, "foo-dc".to_string(), 0, RetryInterval::new("1,2").expect("REASON"),
        );
        let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
            ctxt.init_partition_row_distribution_preset(
                &preset_name, row_count, rows_per_partitions_base, &rows_per_partitions_groups).await
        });

        assert!(matches!(result, Err(ref _e)), "Error result was expected, but got: {:?}", result);
    }

    #[test]
    fn test_partition_row_distribution_preset_06_neg_empty_preset_name() {
        false_input_for_partition_row_distribution_preset("".to_string(), 1000, 10, "100:1".to_string())
    }

    #[test]
    fn test_partition_row_distribution_preset_07_neg_zero_rows() {
        false_input_for_partition_row_distribution_preset("foo".to_string(), 0, 10, "100:1".to_string())
    }

    #[test]
    fn test_partition_row_distribution_preset_08_neg_zero_base() {
        false_input_for_partition_row_distribution_preset("foo".to_string(), 1000, 0, "100:1".to_string())
    }

    #[test]
    fn test_partition_row_distribution_preset_09_neg_percentage_is_less_than_100() {
        false_input_for_partition_row_distribution_preset("foo".to_string(), 1000, 10, "90:1,9.989:2".to_string())
    }

    #[test]
    fn test_partition_row_distribution_preset_10_neg_percentage_is_more_than_100() {
        false_input_for_partition_row_distribution_preset("foo".to_string(), 1000, 10, "90:1,10.011:2".to_string())
    }

    #[test]
    fn test_partition_row_distribution_preset_11_neg_duplicated_percentages() {
        false_input_for_partition_row_distribution_preset("foo".to_string(), 1000, 10, "50:1 , 50:1".to_string())
    }

    #[test]
    fn test_partition_row_distribution_preset_12_neg_wrong_percentages() {
        false_input_for_partition_row_distribution_preset("foo".to_string(), 1000, 10, "90:1,ten:1".to_string())
    }
}
