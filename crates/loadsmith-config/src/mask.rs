/// Tracks secret values that must not appear in logs or printed output.
#[derive(Debug, Default, Clone)]
pub struct MaskList(Vec<String>);

impl MaskList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, secret: impl Into<String>) {
        let s = secret.into();
        if !s.is_empty() {
            self.0.push(s);
        }
    }

    /// Replaces all secret values in `input` with `***`.
    pub fn apply(&self, input: &str) -> String {
        let mut out = input.to_string();
        for secret in &self.0 {
            out = out.replace(secret.as_str(), "***");
        }
        out
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_secrets() {
        let mut list = MaskList::new();
        list.add("s3cr3t");
        list.add("pa$$word");
        assert_eq!(list.apply("user:s3cr3t and pa$$word here"), "user:*** and *** here");
    }

    #[test]
    fn empty_mask_is_noop() {
        let list = MaskList::new();
        assert_eq!(list.apply("hello world"), "hello world");
    }
}
