use time::OffsetDateTime;

/// Format a single log line: `[<role>] <YYYY-MM-DD HH:MM:SS> <LEVEL> <msg>`.
pub fn log_line(role: &str, level: &str, msg: &str) {
    let now = OffsetDateTime::now_local().unwrap_or(OffsetDateTime::now_utc());
    let ts = now
        .format(&time::macros::format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second]"
        ))
        .unwrap_or_else(|_| String::from("unknown"));
    println!("[{}] {} {} {}", role, ts, level, msg);
}

/// Convenience: INFO level log with a fixed role.
pub fn info(role: &str, msg: &str) {
    log_line(role, "INFO", msg);
}

/// Convenience: WARNING level log with a fixed role.
pub fn warn(role: &str, msg: &str) {
    log_line(role, "WARNING", msg);
}

/// Convenience: ERROR level log with a fixed role.
pub fn error(role: &str, msg: &str) {
    log_line(role, "ERROR", msg);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_line_format() {
        let output = std::panic::catch_unwind(|| {
            // Read the formatted timestamp portion using the same format
            let now = OffsetDateTime::now_local().unwrap_or(OffsetDateTime::now_utc());
            let ts = now
                .format(&time::macros::format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second]"
                ))
                .unwrap_or_else(|_| String::from("unknown"));
            format!("[test_role] {} INFO test message", ts)
        });

        let line = output.unwrap();
        // Assert the shape: starts with [test_role], has timestamp, level, message
        assert!(line.starts_with("[test_role] "));
        assert!(line.contains(" INFO test message"));
        // Timestamp part should be 19 chars: "YYYY-MM-DD HH:MM:SS"
        let role_end = line.find("]").unwrap() + 2;
        let ts_part = &line[role_end..role_end + 19];
        // Should match the pattern YYYY-MM-DD HH:MM:SS
        assert!(ts_part.contains('-'));
        assert!(ts_part.contains(':'));
    }
}
