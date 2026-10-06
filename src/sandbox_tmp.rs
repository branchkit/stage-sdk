//! Point Apple's frameworks at the temporary directory the sandbox grants.
//!
//! BranchKit names each confined process's own temporary directory in
//! `$TMPDIR`, a directory under the user's temporary directory, and refuses
//! the rest of it. Apple's frameworks ignore `$TMPDIR` on macOS: they ask
//! `confstr(_CS_DARWIN_USER_TEMP_DIR)`, which answers the shared directory,
//! and writing there is refused. Some stop the process when that happens:
//! Metal's graph compiler, which Core ML uses on the GPU, fails an assertion.
//!
//! libSystem's `_set_user_dir_suffix` moves that answer to a subdirectory of
//! the user's temporary directory. Taking the suffix from `$TMPDIR` lands
//! every framework in the granted directory. It is per-process state, so it
//! has to happen inside the stage, before any framework reads the answer; the
//! runtime does it as the stage starts ([`adopt`]). Unconfined, `$TMPDIR` is
//! the user's temporary directory itself (or unset), and nothing changes.

/// The suffix that moves `base` (the user's temporary directory) onto `own`
/// (`$TMPDIR`), or `None` when `own` is not strictly under `base`.
///
/// Compared as text. The two name the same tree under different spellings:
/// the system answers `/var/folders/...`, and BranchKit names `$TMPDIR` as
/// `/private/var/folders/...` (`/var` is a link to `/private/var`). Resolving
/// the link on disk is not an option, because a confined stage is refused
/// the metadata reads that resolution needs.
pub(crate) fn suffix_under(base: &str, own: &str) -> Option<String> {
    let norm = |p: &str| -> String {
        let p = p.trim_end_matches('/');
        match p.strip_prefix("/private") {
            Some(rest) if rest.starts_with("/var/") => rest.to_string(),
            _ => p.to_string(),
        }
    };
    let (base, own) = (norm(base), norm(own));
    let (base, own) = (base.as_str(), own.as_str());
    let rest = own.strip_prefix(base)?.strip_prefix('/')?;
    (!rest.is_empty()).then(|| rest.to_string())
}

/// Apply the confined process's temporary directory to the frameworks, once
/// per process. A no-op off macOS, unconfined, or if libSystem lacks the call.
pub(crate) fn adopt() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        #[cfg(target_os = "macos")]
        macos::adopt();
    });
}

#[cfg(target_os = "macos")]
mod macos {
    use std::ffi::{CStr, CString};
    use std::os::raw::{c_char, c_int};

    /// `<unistd.h>`'s `_CS_DARWIN_USER_TEMP_DIR`.
    const CS_DARWIN_USER_TEMP_DIR: c_int = 65537;

    type SetSuffix = unsafe extern "C" fn(*const c_char) -> bool;

    pub(super) fn adopt() {
        if let Some(own) = std::env::var_os("TMPDIR") {
            adopt_dir(&own);
        }
    }

    /// [`adopt`] for an explicit `$TMPDIR`.
    pub(super) fn adopt_dir(own: &std::ffi::OsStr) {
        let Some(base) = user_temp_dir() else {
            return;
        };
        let Some(own) = own.to_str() else {
            return;
        };
        let Some(suffix) = super::suffix_under(&base, own) else {
            return;
        };
        let Ok(suffix) = CString::new(suffix) else {
            return;
        };
        // SAFETY: dlsym on the default handle looks up a libSystem export;
        // a null result means the call does not exist and nothing is done.
        let sym = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"_set_user_dir_suffix".as_ptr()) };
        if sym.is_null() {
            return;
        }
        // SAFETY: `_set_user_dir_suffix(const char *)` returns bool; the
        // string outlives the call.
        let set: SetSuffix = unsafe { std::mem::transmute(sym) };
        if !unsafe { set(suffix.as_ptr()) } {
            crate::stage_log::warn(
                "could not move the temporary directory to $TMPDIR; Apple frameworks may be refused theirs",
            );
        }
    }

    /// `confstr(_CS_DARWIN_USER_TEMP_DIR)`, the directory the frameworks use.
    pub(super) fn user_temp_dir() -> Option<String> {
        let mut buf = vec![0 as c_char; 1024];
        // SAFETY: confstr writes at most `buf.len()` bytes, NUL-terminated.
        let n = unsafe { libc::confstr(CS_DARWIN_USER_TEMP_DIR, buf.as_mut_ptr(), buf.len()) };
        if n == 0 || n > buf.len() {
            return None;
        }
        // SAFETY: confstr NUL-terminated what it wrote.
        let s = unsafe { CStr::from_ptr(buf.as_ptr()) };
        s.to_str().ok().map(str::to_string)
    }
}

#[cfg(test)]
mod tests {
    use super::suffix_under;

    #[test]
    fn a_dir_under_the_user_temp_dir_is_its_suffix() {
        let base = "/private/var/folders/ab/xyz/T";
        assert_eq!(
            suffix_under(
                base,
                "/private/var/folders/ab/xyz/T/branchkit/pedal.foot_pedal/"
            ),
            Some("branchkit/pedal.foot_pedal".into())
        );
        assert_eq!(
            suffix_under(&format!("{base}/"), &format!("{base}/x")),
            Some("x".into())
        );
    }

    /// The system answers `/var/...`; BranchKit names `/private/var/...`.
    #[test]
    fn the_var_link_is_one_tree() {
        assert_eq!(
            suffix_under(
                "/var/folders/ab/xyz/T/",
                "/private/var/folders/ab/xyz/T/branchkit/voice.whisperkit_stt/"
            ),
            Some("branchkit/voice.whisperkit_stt".into())
        );
        assert_eq!(
            suffix_under("/private/var/folders/ab/xyz/T", "/var/folders/ab/xyz/T/x"),
            Some("x".into())
        );
        assert_eq!(
            suffix_under("/var/folders/ab/xyz/T", "/private/tmp/x"),
            None
        );
    }

    /// The real call: after it, the directory the frameworks are told is the
    /// one named, as a confined stage's `$TMPDIR` would name it. Process-wide
    /// state, so the directory is real and under the user temp dir, where any
    /// other test's temporary files may then land harmlessly.
    #[cfg(target_os = "macos")]
    #[test]
    fn the_frameworks_are_pointed_at_the_named_dir() {
        let base = super::macos::user_temp_dir().expect("a user temp dir");
        let own = std::path::Path::new(&base)
            .join("branchkit-stage-sdk-test")
            .join(std::process::id().to_string());
        std::fs::create_dir_all(&own).unwrap();
        super::macos::adopt_dir(own.as_os_str());
        let now = super::macos::user_temp_dir().unwrap();
        let canon = |p: &str| std::fs::canonicalize(p).unwrap();
        assert_eq!(canon(&now), canon(own.to_str().unwrap()));
        // As a confined stage is handed it: the /private spelling.
        let private = format!("/private{}", own.display());
        assert!(super::suffix_under(&base, &private).is_some());
        let _ = std::fs::remove_dir(&own);
    }

    #[test]
    fn anything_else_changes_nothing() {
        let base = "/private/var/folders/ab/xyz/T";
        // Unconfined: $TMPDIR is the user temp dir itself.
        assert_eq!(suffix_under(base, &format!("{base}/")), None);
        // A sibling that merely shares the prefix.
        assert_eq!(
            suffix_under(base, "/private/var/folders/ab/xyz/Tmp/x"),
            None
        );
        // Elsewhere entirely.
        assert_eq!(suffix_under(base, "/tmp/x"), None);
    }
}
