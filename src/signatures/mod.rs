/// the signature associated with a trait
pub trait AbstractSignature {

    /// function that generates a checksum of the signature that will tell
    /// wether the versions are compatible
    fn checksum (&self) -> String;

    /// a signature is by default always incompatible with a signature of a differnt sort (different trait)
    fn is_compatible(&self, other: &impl AbstractSignature) -> bool {
        self.checksum() == other.checksum()
    }

}

/// versioned signature
/// a signature that implements sementic versioning
#[derive(Debug, PartialEq)]
pub struct VersionedSignature {
    major_version: usize,
    minor_version: usize,
    patch_version: usize,
}

impl VersionedSignature {

    pub fn new(major: usize, minor: usize, patch: usize) -> VersionedSignature {
        VersionedSignature {major_version: major, minor_version: minor, patch_version: patch}
    }

    /// major version change means breaking change (cache must be cleared)
    pub fn major_version (&self) -> usize {
        self.major_version
    }

    /// minor version change means that a feature has been added to the underlying op
    /// but that the cache is still usable
    pub fn minor_version(&self) -> usize {
        self.minor_version
    }

    /// unexpected/unwanted behaviour has been fixed but for all intents and purposes
    /// the op is the same
    pub fn patch_version(&self) -> usize {
        self.patch_version
    }
}

impl AbstractSignature for VersionedSignature {

    fn checksum(&self) -> String {
        self.major_version().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeSignature;
    impl AbstractSignature for FakeSignature {
        fn checksum(&self) -> String {
            "foobar".to_string()
        }
    }


    #[test]
    fn test_standard_signature() {
        let fake_sign = FakeSignature{};
        let versioned_signature = VersionedSignature::new(1, 0, 0);
        assert_eq!(fake_sign.is_compatible(&versioned_signature), versioned_signature.is_compatible(&fake_sign));
        assert!(!fake_sign.is_compatible(&versioned_signature));
    }

    #[test]
    fn test_versioned_signature() {
        let sing_1 = VersionedSignature::new(1, 0, 0);
        let sing_2 = VersionedSignature::new(1, 3, 2);
        let sing_3 = VersionedSignature::new(2, 0, 1);
        assert_eq!(sing_2.is_compatible(&sing_1), sing_1.is_compatible(&sing_2));
        assert!(sing_2.is_compatible(&sing_1));
        assert!(!sing_2.is_compatible(&sing_3));
    }
}
