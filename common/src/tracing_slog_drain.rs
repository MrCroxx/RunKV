pub struct TracingSlogDrain;

macro_rules! tracing_event {
    ($level:expr, $msg:expr, $filepath:expr, $namespace:expr, $lineno:expr) => {
        match $level {
            tracing::Level::ERROR => tracing::error!(
                code.filepath = $filepath,
                code.namespace = $namespace,
                code.lineno = $lineno,
                "{}",
                $msg
            ),
            tracing::Level::WARN => tracing::warn!(
                code.filepath = $filepath,
                code.namespace = $namespace,
                code.lineno = $lineno,
                "{}",
                $msg
            ),
            tracing::Level::INFO => tracing::info!("{}", $msg),
            tracing::Level::DEBUG => tracing::debug!("{}", $msg),
            tracing::Level::TRACE => tracing::trace!(
                code.filepath = $filepath,
                code.namespace = $namespace,
                code.lineno = $lineno,
                "{}",
                $msg
            ),
        }
    };
}

fn level(level: slog::Level) -> tracing::Level {
    match level {
        // There is not `Critical` level in `tracing`.
        slog::Level::Critical => tracing::Level::ERROR,
        slog::Level::Error => tracing::Level::ERROR,
        slog::Level::Warning => tracing::Level::WARN,
        slog::Level::Info => tracing::Level::INFO,
        slog::Level::Debug => tracing::Level::DEBUG,
        slog::Level::Trace => tracing::Level::TRACE,
    }
}

struct KvSerializer<W: std::io::Write> {
    writer: W,
}

impl<W: std::io::Write> KvSerializer<W> {
    fn new(writer: W) -> Self {
        Self { writer }
    }

    fn into_inner(self) -> W {
        self.writer
    }

    fn write(&mut self, arg: &std::fmt::Arguments) -> slog::Result {
        write!(self.writer, "{}", arg)?;
        Ok(())
    }
}

impl<W: std::io::Write> slog::Serializer for KvSerializer<W> {
    fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments) -> slog::Result {
        write!(self.writer, " {}={}", key, val)?;
        Ok(())
    }
}

impl slog::Drain for TracingSlogDrain {
    type Ok = ();

    type Err = slog::Never;

    fn log(
        &self,
        record: &slog::Record,
        values: &slog::OwnedKVList,
    ) -> std::result::Result<Self::Ok, Self::Err> {
        use slog::KV;

        let writer = std::io::Cursor::new(Vec::new());
        let mut serializer = KvSerializer::new(writer);

        serializer.write(record.msg()).unwrap();
        values.serialize(record, &mut serializer).unwrap();

        let buf = serializer.into_inner().into_inner();
        let s = String::from_utf8_lossy(&buf);

        let level = level(record.level());

        let location = record.location();

        tracing_event!(
            level,
            s.as_ref(),
            location.file,
            location.module,
            location.line
        );
        Ok(())
    }
}
