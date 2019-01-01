extern crate chariots;

#[test]
fn test_add() {
    assert_eq!(chariots::foo_add(1i32, 1i32), 2i32);
}
