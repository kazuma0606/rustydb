use criterion::{criterion_group, criterion_main, Criterion};

fn criterion_bechmark(_c: &mut Criterion) {
    // Your benchmark code here
}

criterion_group!(benches, criterion_bechmark);
criterion_main!(benches);