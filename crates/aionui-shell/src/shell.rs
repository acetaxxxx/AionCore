use std::path::Path;

use aionui_api_types::ToolType;
use tokio::process::Command;

use crate::error::ShellError;

const ALLOWED_URL_SCHEMES: &[&str] = &["http", "https", "mailto"];

#[derive(Default)]
pub struct ShellService;

impl ShellService {
    pub fn new() -> Self {
        Self
    }

    pub async fn open_file(&self, file_path: &str) -> Result<(), ShellError> {
        let path = validate_file_exists(file_path)?;
        open::that_detached(path.as_os_str())
            .map_err(|e| ShellError::CommandFailed(format!("open file: {e}")))?;
        Ok(())
    }

    pub async fn show_item_in_folder(&self, file_path: &str) -> Result<(), ShellError> {
        let path = validate_path_exists(file_path)?;
        show_in_folder(&path).await
    }

    pub async fn open_external(&self, url: &str) -> Result<(), ShellError> {
        validate_url(url)?;
        open::that_detached(url)
            .map_err(|e| ShellError::CommandFailed(format!("open URL: {e}")))?;
        Ok(())
    }

    pub async fn check_tool_installed(&self, tool: ToolType) -> bool {
        match tool {
            ToolType::Terminal | ToolType::Explorer => true,
            ToolType::Vscode => detect_vscode().await,
        }
    }

    pub async fn open_folder_with(
        &self,
        folder_path: &str,
        tool: ToolType,
    ) -> Result<(), ShellError> {
        let path = validate_directory_exists(folder_path)?;
        match tool {
            ToolType::Vscode => open_folder_vscode(&path).await,
            ToolType::Terminal => open_folder_terminal(&path).await,
            ToolType::Explorer => open_folder_explorer(&path).await,
        }
    }
}

fn validate_file_exists(file_path: &str) -> Result<std::path::PathBuf, ShellError> {
    let path = Path::new(file_path);
    let canonical = path
        .canonicalize()
        .map_err(|_| ShellError::FileNotFound(file_path.to_owned()))?;
    if !canonical.is_file() {
        return Err(ShellError::FileNotFound(file_path.to_owned()));
    }
    Ok(canonical)
}

fn validate_path_exists(file_path: &str) -> Result<std::path::PathBuf, ShellError> {
    let path = Path::new(file_path);
    let canonical = path
        .canonicalize()
        .map_err(|_| ShellError::FileNotFound(file_path.to_owned()))?;
    if !canonical.exists() {
        return Err(ShellError::FileNotFound(file_path.to_owned()));
    }
    Ok(canonical)
}

fn validate_directory_exists(dir_path: &str) -> Result<std::path::PathBuf, ShellError> {
    let path = Path::new(dir_path);
    let canonical = path
        .canonicalize()
        .map_err(|_| ShellError::DirectoryNotFound(dir_path.to_owned()))?;
    if !canonical.is_dir() {
        return Err(ShellError::DirectoryNotFound(dir_path.to_owned()));
    }
    Ok(canonical)
}

fn validate_url(url: &str) -> Result<(), ShellError> {
    let parsed = reqwest::Url::parse(url).map_err(|_| ShellError::InvalidUrl(url.to_owned()))?;
    if !ALLOWED_URL_SCHEMES.contains(&parsed.scheme()) {
        return Err(ShellError::InvalidUrl(format!(
            "scheme '{}' is not allowed",
            parsed.scheme()
        )));
    }
    Ok(())
}

async fn show_in_folder(path: &Path) -> Result<(), ShellError> {
    if cfg!(target_os = "macos") {
        run_command("open", &["-R", &path.to_string_lossy()]).await
    } else if cfg!(target_os = "windows") {
        let parent = path.parent().unwrap_or(path);
        run_command("explorer", &[&parent.to_string_lossy()]).await
    } else {
        let parent = path.parent().unwrap_or(path);
        run_command("xdg-open", &[&parent.to_string_lossy()]).await
    }
}

async fn detect_vscode() -> bool {
    if which::which("code").is_ok() {
        return true;
    }
    if cfg!(target_os = "macos") {
        let app_path = "/Applications/Visual Studio Code.app/Contents/Resources/app/bin/code";
        return Path::new(app_path).exists();
    }
    false
}

async fn open_folder_vscode(path: &Path) -> Result<(), ShellError> {
    if !detect_vscode().await {
        return Err(ShellError::ToolNotInstalled("vscode".to_owned()));
    }
    run_command("code", &[&path.to_string_lossy()]).await
}

async fn open_folder_terminal(path: &Path) -> Result<(), ShellError> {
    let path_str = path.to_string_lossy();
    if cfg!(target_os = "macos") {
        run_command("open", &["-a", "Terminal", &path_str]).await
    } else if cfg!(target_os = "windows") {
        run_command(
            "cmd",
            &["/c", "start", "cmd", "/K", &format!("cd /d {path_str}")],
        )
        .await
    } else {
        try_linux_terminal(&path_str).await
    }
}

async fn open_folder_explorer(path: &Path) -> Result<(), ShellError> {
    let path_str = path.to_string_lossy();
    if cfg!(target_os = "macos") {
        run_command("open", &[&path_str]).await
    } else if cfg!(target_os = "windows") {
        run_command("explorer", &[&path_str]).await
    } else {
        run_command("xdg-open", &[&path_str]).await
    }
}

async fn try_linux_terminal(path: &str) -> Result<(), ShellError> {
    let terminals = [
        "gnome-terminal",
        "konsole",
        "xfce4-terminal",
        "x-terminal-emulator",
        "terminator",
    ];
    for term in &terminals {
        if which::which(term).is_ok() {
            let args: Vec<&str> = match *term {
                "gnome-terminal" => vec!["--working-directory", path],
                "konsole" => vec!["--workdir", path],
                _ => vec!["--working-directory", path],
            };
            return run_command(term, &args).await;
        }
    }
    Err(ShellError::ToolNotInstalled("terminal emulator".to_owned()))
}

async fn run_command(program: &str, args: &[&str]) -> Result<(), ShellError> {
    let output = Command::new(program)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| ShellError::CommandFailed(format!("{program}: {e}")))?;

    let result = output
        .wait_with_output()
        .await
        .map_err(|e| ShellError::CommandFailed(format!("{program}: {e}")))?;

    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        tracing::warn!(program, ?args, %stderr, "command exited with non-zero status");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn validate_file_exists_succeeds_for_real_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        fs::write(&file_path, "hello").unwrap();
        let result = validate_file_exists(file_path.to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn validate_file_exists_fails_for_missing_file() {
        let result = validate_file_exists("/nonexistent/file.txt");
        assert!(matches!(result, Err(ShellError::FileNotFound(_))));
    }

    #[test]
    fn validate_file_exists_fails_for_directory() {
        let dir = tempfile::tempdir().unwrap();
        let result = validate_file_exists(dir.path().to_str().unwrap());
        assert!(matches!(result, Err(ShellError::FileNotFound(_))));
    }

    #[test]
    fn validate_path_exists_succeeds_for_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        fs::write(&file_path, "hello").unwrap();
        let result = validate_path_exists(file_path.to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn validate_path_exists_succeeds_for_directory() {
        let dir = tempfile::tempdir().unwrap();
        let result = validate_path_exists(dir.path().to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn validate_path_exists_fails_for_nonexistent() {
        let result = validate_path_exists("/nonexistent/path");
        assert!(matches!(result, Err(ShellError::FileNotFound(_))));
    }

    #[test]
    fn validate_directory_exists_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let result = validate_directory_exists(dir.path().to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn validate_directory_exists_fails_for_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        fs::write(&file_path, "hello").unwrap();
        let result = validate_directory_exists(file_path.to_str().unwrap());
        assert!(matches!(result, Err(ShellError::DirectoryNotFound(_))));
    }

    #[test]
    fn validate_directory_exists_fails_for_nonexistent() {
        let result = validate_directory_exists("/nonexistent/dir");
        assert!(matches!(result, Err(ShellError::DirectoryNotFound(_))));
    }

    #[test]
    fn validate_url_accepts_http() {
        assert!(validate_url("http://example.com").is_ok());
    }

    #[test]
    fn validate_url_accepts_https() {
        assert!(validate_url("https://example.com/path?q=1").is_ok());
    }

    #[test]
    fn validate_url_accepts_mailto() {
        assert!(validate_url("mailto:user@example.com").is_ok());
    }

    #[test]
    fn validate_url_rejects_file_scheme() {
        let result = validate_url("file:///etc/passwd");
        assert!(matches!(result, Err(ShellError::InvalidUrl(msg)) if msg.contains("scheme")));
    }

    #[test]
    fn validate_url_rejects_ftp_scheme() {
        let result = validate_url("ftp://example.com");
        assert!(matches!(result, Err(ShellError::InvalidUrl(msg)) if msg.contains("scheme")));
    }

    #[test]
    fn validate_url_rejects_javascript_scheme() {
        let result = validate_url("javascript:alert(1)");
        assert!(matches!(result, Err(ShellError::InvalidUrl(msg)) if msg.contains("scheme")));
    }

    #[test]
    fn validate_url_rejects_invalid_url() {
        let result = validate_url("; rm -rf /");
        assert!(matches!(result, Err(ShellError::InvalidUrl(_))));
    }

    #[test]
    fn validate_url_rejects_empty_string() {
        let result = validate_url("");
        assert!(matches!(result, Err(ShellError::InvalidUrl(_))));
    }

    #[tokio::test]
    async fn check_tool_terminal_always_true() {
        let svc = ShellService::new();
        assert!(svc.check_tool_installed(ToolType::Terminal).await);
    }

    #[tokio::test]
    async fn check_tool_explorer_always_true() {
        let svc = ShellService::new();
        assert!(svc.check_tool_installed(ToolType::Explorer).await);
    }

    #[tokio::test]
    async fn open_file_fails_for_missing_file() {
        let svc = ShellService::new();
        let result = svc.open_file("/nonexistent/file.txt").await;
        assert!(matches!(result, Err(ShellError::FileNotFound(_))));
    }

    #[tokio::test]
    async fn show_item_in_folder_fails_for_missing_path() {
        let svc = ShellService::new();
        let result = svc.show_item_in_folder("/nonexistent/path").await;
        assert!(matches!(result, Err(ShellError::FileNotFound(_))));
    }

    #[tokio::test]
    async fn open_external_fails_for_invalid_url() {
        let svc = ShellService::new();
        let result = svc.open_external("; rm -rf /").await;
        assert!(matches!(result, Err(ShellError::InvalidUrl(_))));
    }

    #[tokio::test]
    async fn open_external_fails_for_file_scheme() {
        let svc = ShellService::new();
        let result = svc.open_external("file:///etc/passwd").await;
        assert!(matches!(result, Err(ShellError::InvalidUrl(_))));
    }

    #[tokio::test]
    async fn open_folder_with_fails_for_missing_dir() {
        let svc = ShellService::new();
        let result = svc
            .open_folder_with("/nonexistent/dir", ToolType::Explorer)
            .await;
        assert!(matches!(result, Err(ShellError::DirectoryNotFound(_))));
    }

    #[tokio::test]
    async fn open_folder_with_fails_for_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "data").unwrap();
        let svc = ShellService::new();
        let result = svc
            .open_folder_with(file_path.to_str().unwrap(), ToolType::Explorer)
            .await;
        assert!(matches!(result, Err(ShellError::DirectoryNotFound(_))));
    }
}
