// Test File
// N Q
// A1 A2 ... AN

use segtree::SegmentTree;
use std::io::BufRead;

fn main() {
    let mut stdin = std::io::stdin().lock();
    let mut s = String::new();
    stdin.read_line(&mut s).unwrap();
    let [_, q]: [usize; 2] = s.split_whitespace().map(|x| x.parse::<usize>().unwrap()).collect::<Vec<_>>().try_into().unwrap();
    s.clear();
    stdin.read_line(&mut s).unwrap();
    let a = s.split_whitespace().map(|x| x.parse::<i64>().unwrap()).collect::<Vec<_>>();

    let mut seg = SegmentTree::from(&a, |a, b| a + b, 0);

    for _ in 0..q {
        s.clear();
        stdin.read_line(&mut s).unwrap();
        let [t, a, b]: [usize; 3] = s.split_whitespace().map(|x| x.parse::<usize>().unwrap()).collect::<Vec<_>>().try_into().unwrap();
        if t == 0 {
            seg.set(a, seg.get(a) + b as i64);
        } else {
            println!("{}", seg.prod(a..b));
        }
    }
}
