use std::collections::BTreeMap;
use serde::Serialize;
use segtree::SegmentTree;
use server::ObservationPointData;

const WINDOW_SIZE: usize = 365;

type FloatSegTree = SegmentTree<f64, fn(&f64, &f64) -> f64>;

#[derive(Serialize)]
pub struct WindowAggregateResult {
    max: f64,
    min: f64,
    avg: f64,
}

impl TryFrom<&PointAggregator> for WindowAggregateResult {
    type Error = ();

    fn try_from(value: &PointAggregator) -> Result<Self, Self::Error> {
        if value.count == 0 {
            Err(())
        } else {
            Ok(Self {
                max: value.max().unwrap(),
                min: value.min().unwrap(),
                avg: value.average().unwrap(),
            })
        }
    }
}

struct PointAggregator {
    records: [f64; WINDOW_SIZE],
    count: usize,
    index: usize,
    sum: f64,
    max_seg: FloatSegTree,
    min_seg: FloatSegTree,
}

impl PointAggregator {
    pub fn new() -> Self {
        Self {
            records: [0.0; WINDOW_SIZE],
            count: 0,
            index: 0,
            sum: 0.0,
            max_seg: FloatSegTree::new(WINDOW_SIZE, |&a, &b| a.max(b), f64::MIN),
            min_seg: FloatSegTree::new(WINDOW_SIZE, |&a, &b| a.min(b), f64::MAX),
        }
    }

    pub fn add(&mut self, min: f64, max: f64, avg: f64) {
        self.sum -= self.records[self.index];
        self.records[self.index] = avg;
        self.sum += avg;
        self.count += 1;
        self.max_seg.set(self.index, max);
        self.min_seg.set(self.index, min);
        
        self.index = (self.index + 1) % WINDOW_SIZE;
    }
    
    pub fn max(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.max_seg.all_prod())
        }
    }

    pub fn min(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.min_seg.all_prod())
        }
    }

    pub fn average(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum / self.count.min(WINDOW_SIZE) as f64)
        }
    }
}

pub struct WindowAggregator {
    points: BTreeMap<u32, PointAggregator>
}

impl WindowAggregator {
    pub fn new() -> WindowAggregator {
        Self {
            points: BTreeMap::new()
        }
    }

    pub fn add(&mut self, data: &Vec<ObservationPointData>) {
        for point in data {
            let aggr = self.points.entry(point.point_id()).or_insert_with(|| PointAggregator::new());
            aggr.add(point.min(), point.max(), point.average());
        }
    }

    pub fn to_vec(&self) -> Vec<(u32, WindowAggregateResult)> {
        self.points.iter()
            .filter(|(_, v)| v.count > 0)
            .map(|(k, v)| (*k, WindowAggregateResult::try_from(v).unwrap()) )
            .collect()
    }
}
