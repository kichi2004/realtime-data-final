use crate::AppState;
use bytes::Bytes;
use chrono::{Datelike, NaiveDate};
use serde::{Serialize, Serializer};
use server::decompress_data;
use std::collections::BTreeMap;
use std::sync::Arc;
use crate::prefecture::get_prefecture_code;
use crate::window_aggregator::WindowAggregator;

#[derive(Clone, Debug)]
struct MonthAggregateResult {
    min: f64,
    max: f64,
    sum: f64,
    count: usize,
    // 猛暑日
    high_over_35: usize,
    // 真夏日
    high_over_30: usize,
    // 夏日
    high_over_25: usize,
    // 熱帯夜
    low_over_25: usize,
    // 真冬日
    high_below_0: usize,
    // 冬日
    low_below_0: usize,
}

impl MonthAggregateResult {
    fn new() -> Self {
        Self {
            min: f64::MAX,
            max: f64::MIN,
            sum: 0.0,
            count: 0,
            high_over_35: 0,
            high_over_30: 0,
            high_over_25: 0,
            low_over_25: 0,
            high_below_0: 0,
            low_below_0: 0,
        }
    }

    fn add(&mut self, min: f64, max: f64, avg: f64) {
        self.min = self.min.min(min);
        self.max = self.max.max(max);
        self.sum += avg;
        self.count += 1;
        if max >= 35.0 {
            self.high_over_35 += 1;
        } else if max >= 30.0 {
            self.high_over_30 += 1;
        } else if max >= 25.0 {
            self.high_over_25 += 1;
        }
        if min >= 25.0 {
            self.low_over_25 += 1;
        }
        if max < 0.0 {
            self.high_below_0 += 1;
        } else if min < 0.0 {
            self.low_below_0 += 1;
        }
    }

    fn average(&self) -> Option<f64> {
        if self.count == 0 { None }
        else { Some(self.sum / self.count as f64) }
    }
}

impl Serialize for MonthAggregateResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("MonthAggregateResult", 10)?;
        state.serialize_field("min", &self.min)?;
        state.serialize_field("max", &self.max)?;
        state.serialize_field("count", &self.count)?;
        state.serialize_field("highOver35", &self.high_over_35)?;
        state.serialize_field("highOver30", &self.high_over_30)?;
        state.serialize_field("highOver25", &self.high_over_25)?;
        state.serialize_field("lowOver25", &self.low_over_25)?;
        state.serialize_field("highBelow0", &self.high_below_0)?;
        state.serialize_field("lowBelow0", &self.low_below_0)?;
        state.serialize_field("average", &self.average())?;
        state.end()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PointAggregateResult {
    min: f64,
    max: f64,
    sum: f64,
    count: usize,
    latest_date: [u32; 3],
    months: [MonthAggregateResult; 12]
}

impl PointAggregateResult {
    fn new() -> Self {
        Self { min: f64::MAX, max: f64::MIN, sum: 0.0, count: 0, latest_date: [1900, 1, 1], months: vec![MonthAggregateResult::new(); 12].try_into().unwrap() }
    }
    
    fn add(&mut self, date: NaiveDate, min: f64, max: f64, avg: f64) {
        self.min = self.min.min(min);
        self.max = self.max.max(max);
        self.sum += avg;
        self.count += 1;
        self.latest_date = [date.year().cast_unsigned(), date.month(), date.day()];
        self.months[date.month0() as usize].add(min, max, avg);
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PrefectureAggregateResult {
    id: u32,
    name: String,
    prefecture_name: String,
    min: f64,
    max: f64,
    sum: f64,
    count: usize,
    latest_date: [u32; 3],
    // 猛暑日
    high_over_35: usize,
    // 真夏日
    high_over_30: usize,
    // 夏日
    high_over_25: usize,
    // 熱帯夜
    low_over_25: usize,
    // 真冬日
    high_below_0: usize,
    // 冬日
    low_below_0: usize,
}

impl PrefectureAggregateResult {
    fn new(id: u32, name: String, prefecture_name: String) -> Self {
        Self { id, name, prefecture_name, latest_date: [1900, 1, 1],
            min: f64::MAX, max: f64::MIN, sum: 0.0, count: 0,
            high_over_35: 0, high_over_30: 0, high_over_25: 0,
            low_over_25: 0, high_below_0: 0, low_below_0: 0 }
    }

    fn add(&mut self, date: NaiveDate, min: f64, max: f64, avg: f64) {
        self.min = self.min.min(min);
        self.max = self.max.max(max);
        self.sum += avg;
        self.count += 1;
        if max >= 35.0 {
            self.high_over_35 += 1;
        } else if max >= 30.0 {
            self.high_over_30 += 1;
        } else if max >= 25.0 {
            self.high_over_25 += 1;
        }
        if min >= 25.0 {
            self.low_over_25 += 1;
        }
        if max < 0.0 {
            self.high_below_0 += 1;
        } else if min < 0.0 {
            self.low_below_0 += 1;
        }
        self.latest_date = [date.year().cast_unsigned(), date.month(), date.day()];
    }
}

/*
地点名を選んで、月（暦月）ごとの統計を表示。（最低、最高、平均、夏日や熱帯夜など）

日較差でグラフ？
 */
pub(crate) struct Aggregator {
    state: Arc<AppState>,
    aggregate_by_point: BTreeMap<u32, PointAggregateResult>,
    aggregate_by_prefecture: BTreeMap<u32, PrefectureAggregateResult>,
    window_aggregator: WindowAggregator
}

impl Aggregator {
    pub fn new(state: Arc<AppState>) -> Self {
        Self {
            aggregate_by_prefecture: state.observation_points.iter()
                .filter(|&p| p.is_prefecture_center())
                .map(|p| (get_prefecture_code(p.prefecture()), PrefectureAggregateResult::new(p.id(), p.name().to_string(), p.prefecture().to_string())))
                .collect(),
            aggregate_by_point: BTreeMap::new(),
            state,
            window_aggregator: WindowAggregator::new()
        }
    }

    pub fn on_receive_data(&mut self, binary: Bytes) -> Result<(), anyhow::Error>{
        let (date, data) = decompress_data(&binary);
        self.window_aggregator.add(&data);
        for point_data in data {
            if !self.aggregate_by_point.contains_key(&point_data.point_id()) {
                self.aggregate_by_point.insert(
                    point_data.point_id(),
                    PointAggregateResult::new()
                );
            }
            self.aggregate_by_point.get_mut(&point_data.point_id()).unwrap().add(
                date, point_data.min(), point_data.max(), point_data.average()
            );

            let point = self.state.observation_point_map.get(&point_data.point_id()).unwrap();
            if point.is_prefecture_center() {
                let pref = get_prefecture_code(point.prefecture());
                self.aggregate_by_prefecture.get_mut(&pref).map(|x|
                    x.add(
                        date, point_data.min(), point_data.max(), point_data.average(),
                    ));
            }
        }

        self.state.get_tx(0).send(vec![(0, binary)])?;
        self.state.get_tx(1).send(
            self.aggregate_by_point
                .iter()
                .map(|(id, point)|
                    (*id, rmp_serde::to_vec_named(point).unwrap().into()))
                .collect()
        )?;
        let bytes = rmp_serde::to_vec_named(&self.aggregate_by_prefecture)?;
        self.state.get_tx(2).send(vec![(0, bytes.into())])?;
        let bytes = rmp_serde::to_vec(&(
            [date.year().cast_unsigned(), date.month(), date.day()],
            self.window_aggregator.to_vec()
        ))?;
        self.state.get_tx(3).send(vec![(0, bytes.into())])?;
        Ok(())
    }
}
