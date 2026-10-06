use super::Error;

#[derive(Debug)]
#[repr(transparent)]
pub struct Path([u8]);

impl Path {
    pub fn components(&self) -> impl Iterator<Item = &[u8]> {
        self.0.split(|b| *b == b'/')
    }

    #[must_use]
    pub fn is_dir(&self) -> bool {
        self.0.ends_with(b"/")
    }

    /// # Errors
    pub fn new(path: &[u8]) -> Result<&Path, Error> {
        if Self::is_valid(path) {
            // SAFETY: Path is transparent on [u8]
            Ok(unsafe { &*(std::ptr::from_ref::<[u8]>(path) as *const Path) })
        } else {
            Err(Error::InvalidPath)
        }
    }

    #[must_use]
    pub fn is_valid(path: &[u8]) -> bool {
        !path.is_empty()
            && path[0] == b'/'
            && path.windows(2).all(|w| w[0] != b'/' || w[1] != b'/')
            && path
                .iter()
                .all(|c| c.is_ascii_alphanumeric() || b"._-/".contains(c))
    }
}

impl<'a> TryFrom<&'a str> for &'a Path {
    type Error = Error;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        Path::new(value.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid() {
        Path::new("/e4oq1-HLbqZW77rFpqerxYCBm7oPI-mkWXXYLC5C6-KRPp7lwXb_f_YUyVH-t4w53oKLzUhKTmqcfdHV_zX8ESbJv9tghYEnoew_lXGVEirTq3t.m_NyUMbW-pq9zXYpRwuDoK7pTtP47mhD-vlbnVLkspE5/cDoC.yg/-wagwgLrifbiR9vkdcD5y5-XZ1e6T/eRXRuLhcRecptRO6sx.GCoBw9KgRXWEKfgvWKeEODullCe".as_bytes()).expect("valid path");
    }
}
