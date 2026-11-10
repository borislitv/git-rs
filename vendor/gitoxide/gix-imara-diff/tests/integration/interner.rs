use std::cell::Cell;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use gix_imara_diff::Interner;

#[test]
fn borrowed_hits_do_not_acquire_another_owned_token() {
    struct CountedToken {
        text: &'static str,
        copies: Rc<Cell<usize>>,
    }

    impl Clone for CountedToken {
        fn clone(&self) -> Self {
            self.copies.set(self.copies.get() + 1);
            Self {
                text: self.text,
                copies: Rc::clone(&self.copies),
            }
        }
    }

    impl Hash for CountedToken {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.text.hash(state);
        }
    }

    impl PartialEq for CountedToken {
        fn eq(&self, other: &Self) -> bool {
            self.text == other.text
        }
    }

    impl Eq for CountedToken {}

    let copies = Rc::new(Cell::new(0));
    let input = CountedToken {
        text: "repeated line\n",
        copies: Rc::clone(&copies),
    };
    let mut interner = Interner::new(0);
    let token = interner.intern_ref(&input);
    assert_eq!(copies.get(), 1, "a new token needs one owned value");
    for _ in 0..64 {
        assert_eq!(interner.intern_ref(&input), token);
    }
    assert_eq!(copies.get(), 1, "existing tokens must not be cloned on lookup");
    assert_eq!(interner.num_tokens(), 1);
    assert_eq!(interner[token].text, "repeated line\n");
}

#[test]
fn borrowed_and_owned_byte_tokens_share_identity_after_growth_and_erasure() {
    let mut interner = Interner::<Vec<u8>>::new(0);
    let empty = interner.intern_ref(&b""[..]);
    let binary = interner.intern(b"\xff\0\r\n".to_vec());
    assert_eq!(interner.intern_ref(&b"\xff\0\r\n"[..]), binary);
    for value in 0..512u32 {
        interner.intern_ref(&value.to_le_bytes()[..]);
    }
    interner.reserve(1024);
    assert_eq!(interner.intern_ref(&b""[..]), empty);
    assert_eq!(interner.intern(b"\xff\0\r\n".to_vec()), binary);
    let suffix = interner.intern_ref(&b"last\n"[..]);
    interner.erase_tokens_after(suffix);
    assert_eq!(interner.intern_ref(&b"last\n"[..]), suffix);
    assert_eq!(interner[binary], b"\xff\0\r\n");
    interner.clear();
    assert_eq!(interner.num_tokens(), 0);
    assert_eq!(interner.intern_ref(&b"last\n"[..]).0, 0);
}

#[test]
fn borrowed_tokens_with_equal_hashes_still_require_equal_content() {
    #[derive(Clone, PartialEq, Eq)]
    struct Colliding(u32);

    impl Hash for Colliding {
        fn hash<H: Hasher>(&self, state: &mut H) {
            0u8.hash(state);
        }
    }

    let mut interner = Interner::new(0);
    let first = interner.intern_ref(&Colliding(1));
    let second = interner.intern_ref(&Colliding(2));
    assert_ne!(first, second, "hash equality must not merge distinct tokens");
    assert_eq!(interner.intern(Colliding(1)), first);
    assert_eq!(interner.intern_ref(&Colliding(2)), second);
    assert_eq!(interner[first].0, 1);
    assert_eq!(interner[second].0, 2);
}
