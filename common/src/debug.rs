macro_rules! time {
    ($name:literal, $block:block) => {{
        let now = std::time::Instant::now();
        let result = $block;
        tracing::debug!("{}: {}ms", $name, now.elapsed().as_millis());
        result
    }};
}

pub(crate) use time;
