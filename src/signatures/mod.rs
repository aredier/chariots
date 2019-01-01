/// the signature associated with a trait
pub trait AbstractSignature {

    /// wether this signature is compatible with the other signature
    fn is_compatible<S: AbstractSignature> (&self, other: S) -> bool;

}
