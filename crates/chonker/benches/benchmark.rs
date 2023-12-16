use chonker::EncodeContext;
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;

pub fn create(c: &mut Criterion) {
    c.bench_function("chonker::Archive::create", |b| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        b.to_async(runtime).iter_custom(move |iters| {
            let cargo_manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
            let path = std::path::Path::new(&cargo_manifest_dir).join("../../../chonker-assets/source.tar");
            let reader = std::fs::read(path).unwrap();
            let reader = std::sync::Arc::<[u8]>::from(reader.into_boxed_slice());
            let reader_size = reader.len();
            let reader = std::io::Cursor::new(reader);
            async move {
                let mut writer = vec![u8::default(); reader_size];
                let reader_size = u64::try_from(reader_size).unwrap().into();
                let mut total_time = std::time::Duration::default();
                let context = Arc::<EncodeContext>::default();
                for _i in 0 .. iters {
                    let context = context.clone();
                    let reader = reader.clone();
                    writer.clear();
                    let start = std::time::Instant::now();
                    chonker::Archive::create(context, reader, reader_size, &mut writer)
                        .await
                        .unwrap();
                    total_time += start.elapsed();
                }
                total_time
            }
        })
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = create
);
criterion_main!(benches);
