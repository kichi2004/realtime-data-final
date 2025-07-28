use std::ops::Range;

pub struct SegmentTree<T, F>
where
    T: Copy,
    F: Fn(&T, &T) -> T,
{
    n: usize,
    size: usize,
    log: usize,
    data: Vec<T>,
    id: T,
    op: F,
}

impl<T : Copy, F: Fn(&T, &T) -> T> SegmentTree<T, F> {
    pub fn new(n: usize, op: F, id: T) -> Self {
        Self::from(&vec![id; n], op, id)
    }

    pub fn from(value: &[T], op: F, id: T) -> Self {
        let n = value.len();
        let size = value.len().next_power_of_two();
        let log = size.trailing_zeros() as usize;
        let mut data = vec![id; 2 * size];
        for i in 0..n {
            data[size + i] = value[i];
        }
        let mut seg = Self { n, size, log, data, id, op };
        for i in (1..size).rev() {
            seg.update(i);
        }
        seg
    }

    pub fn set(&mut self, mut p: usize, x: T) {
        assert!(p < self.n);
        p += self.size;
        self.data[p] = x;
        for i in 1..=self.log {
            self.update(p >> i);
        }
    }

    pub fn get(&self, p: usize) -> T {
        assert!(p < self.n);
        self.data[p + self.size]
    }

    pub fn prod(&self, range: Range<usize>) -> T {
        assert!(range.end <= self.n);
        let mut sml = self.id;
        let mut smr = self.id;
        let mut l = range.start + self.size;
        let mut r = range.end + self.size;

        while l < r {
            if (l & 1) != 0 {
                sml = (self.op)(&sml, &self.data[l]);
                l += 1;
            }
            if (r & 1) != 0 {
                r -= 1;
                smr = (self.op)(&self.data[r], &smr);
            }
            l >>= 1;
            r >>= 1;
        }
        (self.op)(&sml, &smr)
    }

    pub fn all_prod(&self) -> T {
        self.data[1]
    }

    fn update(&mut self, k: usize) {
        self.data[k] = (self.op)(&self.data[2 * k], &self.data[2 * k + 1]);
    }
}
