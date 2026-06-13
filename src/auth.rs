use anyhow::Result;

/// Write a Robinhood access token (and optional refresh token) into .env.
/// Used by `hoodrat auth <token>` and by bot.rs on 401 re-auth prompts.
pub fn write_token(access_token: &str, refresh_token: Option<&str>) -> Result<()> {
    let path = std::path::Path::new(".env");
    let existing = if path.exists() { std::fs::read_to_string(path)? } else { String::new() };
    let mut lines: Vec<String> = existing.lines().map(str::to_string).collect();
    upsert_env_line(&mut lines, "ROBINHOOD_ACCESS_TOKEN", access_token);
    if let Some(rt) = refresh_token {
        upsert_env_line(&mut lines, "ROBINHOOD_REFRESH_TOKEN", rt);
    }
    std::fs::write(path, lines.join("\n") + "\n")?;
    Ok(())
}

fn upsert_env_line(lines: &mut Vec<String>, key: &str, value: &str) {
    let prefix = format!("{key}=");
    if let Some(pos) = lines.iter().position(|l| l.starts_with(&prefix)) {
        lines[pos] = format!("{key}={value}");
    } else {
        lines.push(format!("{key}={value}"));
    }
}
