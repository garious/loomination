//! A C representation of Rust's `std::option::Option` used accross the FFI
//! boundary for Solana program interfaces
//!
//! This implementation mostly matches `std::option` except iterators since the iteration
//! trait requires returning `std::option::Option`

use std::{
    convert, mem,
    ops::{Deref, DerefMut},
};

/// A C representation of Rust's `std::option::Option`
#[repr(C)]
#[derive(Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Hash)]
pub enum COption<T> {
    /// No value
    None,
    /// Some value `T`
    Some(T),
}

/////////////////////////////////////////////////////////////////////////////
// Type implementation
/////////////////////////////////////////////////////////////////////////////

impl<T> COption<T> {
    /////////////////////////////////////////////////////////////////////////
    // Querying the contained values
    /////////////////////////////////////////////////////////////////////////

    /// Returns `true` if the option is a [`COption::Some`] value.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x: COption<u32> = COption::Some(2);
    /// assert_eq!(x.is_some(), true);
    ///
    /// let x: COption<u32> = COption::None;
    /// assert_eq!(x.is_some(), false);
    /// ```
    ///
    /// [`COption::Some`]: #variant.COption::Some
    #[must_use = "if you intended to assert that this has a value, consider `.unwrap()` instead"]
    #[inline]
    pub fn is_some(&self) -> bool {
        match *self {
            COption::Some(_) => true,
            COption::None => false,
        }
    }

    /// Returns `true` if the option is a [`COption::None`] value.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x: COption<u32> = COption::Some(2);
    /// assert_eq!(x.is_none(), false);
    ///
    /// let x: COption<u32> = COption::None;
    /// assert_eq!(x.is_none(), true);
    /// ```
    ///
    /// [`COption::None`]: #variant.COption::None
    #[must_use = "if you intended to assert that this doesn't have a value, consider \
                  `.and_then(|| panic!(\"`COption` had a value when expected `COption::None`\"))` instead"]
    #[inline]
    pub fn is_none(&self) -> bool {
        !self.is_some()
    }

    /// Returns `true` if the option is a [`COption::Some`] value containing the given value.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// #![feature(option_result_contains)]
    ///
    /// let x: COption<u32> = COption::Some(2);
    /// assert_eq!(x.contains(&2), true);
    ///
    /// let x: COption<u32> = COption::Some(3);
    /// assert_eq!(x.contains(&2), false);
    ///
    /// let x: COption<u32> = COption::None;
    /// assert_eq!(x.contains(&2), false);
    /// ```
    #[must_use]
    #[inline]
    pub fn contains<U>(&self, x: &U) -> bool
    where
        U: PartialEq<T>,
    {
        match self {
            COption::Some(y) => x == y,
            COption::None => false,
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // Adapter for working with references
    /////////////////////////////////////////////////////////////////////////

    /// Converts from `&COption<T>` to `COption<&T>`.
    ///
    /// # Examples
    ///
    /// Converts an `COption<`[`String`]`>` into an `COption<`[`usize`]`>`, preserving the original.
    /// The [`map`] method takes the `self` argument by value, consuming the original,
    /// so this technique uses `as_ref` to first take an `COption` to a reference
    /// to the value inside the original.
    ///
    /// [`map`]: enum.COption.html#method.map
    /// [`String`]: ../../std/string/struct.String.html
    /// [`usize`]: ../../std/primitive.usize.html
    ///
    /// ```ignore
    /// let text: COption<String> = COption::Some("Hello, world!".to_string());
    /// // First, cast `COption<String>` to `COption<&String>` with `as_ref`,
    /// // then consume *that* with `map`, leaving `text` on the stack.
    /// let text_length: COption<usize> = text.as_ref().map(|s| s.len());
    /// println!("still can print text: {:?}", text);
    /// ```
    #[inline]
    pub fn as_ref(&self) -> COption<&T> {
        match *self {
            COption::Some(ref x) => COption::Some(x),
            COption::None => COption::None,
        }
    }

    /// Converts from `&mut COption<T>` to `COption<&mut T>`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut x = COption::Some(2);
    /// match x.as_mut() {
    ///     COption::Some(v) => *v = 42,
    ///     COption::None => {},
    /// }
    /// assert_eq!(x, COption::Some(42));
    /// ```
    #[inline]
    pub fn as_mut(&mut self) -> COption<&mut T> {
        match *self {
            COption::Some(ref mut x) => COption::Some(x),
            COption::None => COption::None,
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // Getting to contained values
    /////////////////////////////////////////////////////////////////////////

    /// Unwraps an option, yielding the content of a [`COption::Some`].
    ///
    /// # Panics
    ///
    /// Panics if the value is a [`COption::None`] with a custom panic message provided by
    /// `msg`.
    ///
    /// [`COption::Some`]: #variant.COption::Some
    /// [`COption::None`]: #variant.COption::None
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = COption::Some("value");
    /// assert_eq!(x.expect("the world is ending"), "value");
    /// ```
    ///
    /// ```ignore{.should_panic}
    /// let x: COption<&str> = COption::None;
    /// x.expect("the world is ending"); // panics with `the world is ending`
    /// ```
    #[inline]
    pub fn expect(self, msg: &str) -> T {
        match self {
            COption::Some(val) => val,
            COption::None => expect_failed(msg),
        }
    }

    /// Moves the value `v` out of the `COption<T>` if it is [`COption::Some(v)`].
    ///
    /// In general, because this function may panic, its use is discouraged.
    /// Instead, prefer to use pattern matching and handle the [`COption::None`]
    /// case explicitly.
    ///
    /// # Panics
    ///
    /// Panics if the self value equals [`COption::None`].
    ///
    /// [`COption::Some(v)`]: #variant.COption::Some
    /// [`COption::None`]: #variant.COption::None
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = COption::Some("air");
    /// assert_eq!(x.unwrap(), "air");
    /// ```
    ///
    /// ```ignore{.should_panic}
    /// let x: COption<&str> = COption::None;
    /// assert_eq!(x.unwrap(), "air"); // fails
    /// ```
    #[inline]
    pub fn unwrap(self) -> T {
        match self {
            COption::Some(val) => val,
            COption::None => panic!("called `COption::unwrap()` on a `COption::None` value"),
        }
    }

    /// Returns the contained value or a default.
    ///
    /// Arguments passed to `unwrap_or` are eagerly evaluated; if you are passing
    /// the result of a function call, it is recommended to use [`unwrap_or_else`],
    /// which is lazily evaluated.
    ///
    /// [`unwrap_or_else`]: #method.unwrap_or_else
    ///
    /// # Examples
    ///
    /// ```ignore
    /// assert_eq!(COption::Some("car").unwrap_or("bike"), "car");
    /// assert_eq!(COption::None.unwrap_or("bike"), "bike");
    /// ```
    #[inline]
    pub fn unwrap_or(self, def: T) -> T {
        match self {
            COption::Some(x) => x,
            COption::None => def,
        }
    }

    /// Returns the contained value or computes it from a closure.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let k = 10;
    /// assert_eq!(COption::Some(4).unwrap_or_else(|| 2 * k), 4);
    /// assert_eq!(COption::None.unwrap_or_else(|| 2 * k), 20);
    /// ```
    #[inline]
    pub fn unwrap_or_else<F: FnOnce() -> T>(self, f: F) -> T {
        match self {
            COption::Some(x) => x,
            COption::None => f(),
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // Transforming contained values
    /////////////////////////////////////////////////////////////////////////

    /// Maps an `COption<T>` to `COption<U>` by applying a function to a contained value.
    ///
    /// # Examples
    ///
    /// Converts an `COption<`[`String`]`>` into an `COption<`[`usize`]`>`, consuming the original:
    ///
    /// [`String`]: ../../std/string/struct.String.html
    /// [`usize`]: ../../std/primitive.usize.html
    ///
    /// ```ignore
    /// let maybe_some_string = COption::Some(String::from("Hello, World!"));
    /// // `COption::map` takes self *by value*, consuming `maybe_some_string`
    /// let maybe_some_len = maybe_some_string.map(|s| s.len());
    ///
    /// assert_eq!(maybe_some_len, COption::Some(13));
    /// ```
    #[inline]
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> COption<U> {
        match self {
            COption::Some(x) => COption::Some(f(x)),
            COption::None => COption::None,
        }
    }

    /// Applies a function to the contained value (if any),
    /// or returns the provided default (if not).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = COption::Some("foo");
    /// assert_eq!(x.map_or(42, |v| v.len()), 3);
    ///
    /// let x: COption<&str> = COption::None;
    /// assert_eq!(x.map_or(42, |v| v.len()), 42);
    /// ```
    #[inline]
    pub fn map_or<U, F: FnOnce(T) -> U>(self, default: U, f: F) -> U {
        match self {
            COption::Some(t) => f(t),
            COption::None => default,
        }
    }

    /// Applies a function to the contained value (if any),
    /// or computes a default (if not).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let k = 21;
    ///
    /// let x = COption::Some("foo");
    /// assert_eq!(x.map_or_else(|| 2 * k, |v| v.len()), 3);
    ///
    /// let x: COption<&str> = COption::None;
    /// assert_eq!(x.map_or_else(|| 2 * k, |v| v.len()), 42);
    /// ```
    #[inline]
    pub fn map_or_else<U, D: FnOnce() -> U, F: FnOnce(T) -> U>(self, default: D, f: F) -> U {
        match self {
            COption::Some(t) => f(t),
            COption::None => default(),
        }
    }

    /// Transforms the `COption<T>` into a [`Result<T, E>`], mapping [`COption::Some(v)`] to
    /// [`Ok(v)`] and [`COption::None`] to [`Err(err)`].
    ///
    /// Arguments passed to `ok_or` are eagerly evaluated; if you are passing the
    /// result of a function call, it is recommended to use [`ok_or_else`], which is
    /// lazily evaluated.
    ///
    /// [`Result<T, E>`]: ../../std/result/enum.Result.html
    /// [`Ok(v)`]: ../../std/result/enum.Result.html#variant.Ok
    /// [`Err(err)`]: ../../std/result/enum.Result.html#variant.Err
    /// [`COption::None`]: #variant.COption::None
    /// [`COption::Some(v)`]: #variant.COption::Some
    /// [`ok_or_else`]: #method.ok_or_else
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = COption::Some("foo");
    /// assert_eq!(x.ok_or(0), Ok("foo"));
    ///
    /// let x: COption<&str> = COption::None;
    /// assert_eq!(x.ok_or(0), Err(0));
    /// ```
    #[inline]
    pub fn ok_or<E>(self, err: E) -> Result<T, E> {
        match self {
            COption::Some(v) => Ok(v),
            COption::None => Err(err),
        }
    }

    /// Transforms the `COption<T>` into a [`Result<T, E>`], mapping [`COption::Some(v)`] to
    /// [`Ok(v)`] and [`COption::None`] to [`Err(err())`].
    ///
    /// [`Result<T, E>`]: ../../std/result/enum.Result.html
    /// [`Ok(v)`]: ../../std/result/enum.Result.html#variant.Ok
    /// [`Err(err())`]: ../../std/result/enum.Result.html#variant.Err
    /// [`COption::None`]: #variant.COption::None
    /// [`COption::Some(v)`]: #variant.COption::Some
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = COption::Some("foo");
    /// assert_eq!(x.ok_or_else(|| 0), Ok("foo"));
    ///
    /// let x: COption<&str> = COption::None;
    /// assert_eq!(x.ok_or_else(|| 0), Err(0));
    /// ```
    #[inline]
    pub fn ok_or_else<E, F: FnOnce() -> E>(self, err: F) -> Result<T, E> {
        match self {
            COption::Some(v) => Ok(v),
            COption::None => Err(err()),
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // Boolean operations on the values, eager and lazy
    /////////////////////////////////////////////////////////////////////////

    /// Returns [`COption::None`] if the option is [`COption::None`], otherwise returns `optb`.
    ///
    /// [`COption::None`]: #variant.COption::None
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = COption::Some(2);
    /// let y: COption<&str> = COption::None;
    /// assert_eq!(x.and(y), COption::None);
    ///
    /// let x: COption<u32> = COption::None;
    /// let y = COption::Some("foo");
    /// assert_eq!(x.and(y), COption::None);
    ///
    /// let x = COption::Some(2);
    /// let y = COption::Some("foo");
    /// assert_eq!(x.and(y), COption::Some("foo"));
    ///
    /// let x: COption<u32> = COption::None;
    /// let y: COption<&str> = COption::None;
    /// assert_eq!(x.and(y), COption::None);
    /// ```
    #[inline]
    pub fn and<U>(self, optb: COption<U>) -> COption<U> {
        match self {
            COption::Some(_) => optb,
            COption::None => COption::None,
        }
    }

    /// Returns [`COption::None`] if the option is [`COption::None`], otherwise calls `f` with the
    /// wrapped value and returns the result.
    ///
    /// COption::Some languages call this operation flatmap.
    ///
    /// [`COption::None`]: #variant.COption::None
    ///
    /// # Examples
    ///
    /// ```ignore
    /// fn sq(x: u32) -> COption<u32> { COption::Some(x * x) }
    /// fn nope(_: u32) -> COption<u32> { COption::None }
    ///
    /// assert_eq!(COption::Some(2).and_then(sq).and_then(sq), COption::Some(16));
    /// assert_eq!(COption::Some(2).and_then(sq).and_then(nope), COption::None);
    /// assert_eq!(COption::Some(2).and_then(nope).and_then(sq), COption::None);
    /// assert_eq!(COption::None.and_then(sq).and_then(sq), COption::None);
    /// ```
    #[inline]
    pub fn and_then<U, F: FnOnce(T) -> COption<U>>(self, f: F) -> COption<U> {
        match self {
            COption::Some(x) => f(x),
            COption::None => COption::None,
        }
    }

    /// Returns [`COption::None`] if the option is [`COption::None`], otherwise calls `predicate`
    /// with the wrapped value and returns:
    ///
    /// - [`COption::Some(t)`] if `predicate` returns `true` (where `t` is the wrapped
    ///   value), and
    /// - [`COption::None`] if `predicate` returns `false`.
    ///
    /// This function works similar to [`Iterator::filter()`]. You can imagine
    /// the `COption<T>` being an iterator over one or zero elements. `filter()`
    /// lets you decide which elements to keep.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// fn is_even(n: &i32) -> bool {
    ///     n % 2 == 0
    /// }
    ///
    /// assert_eq!(COption::None.filter(is_even), COption::None);
    /// assert_eq!(COption::Some(3).filter(is_even), COption::None);
    /// assert_eq!(COption::Some(4).filter(is_even), COption::Some(4));
    /// ```
    ///
    /// [`COption::None`]: #variant.COption::None
    /// [`COption::Some(t)`]: #variant.COption::Some
    /// [`Iterator::filter()`]: ../../std/iter/trait.Iterator.html#method.filter
    #[inline]
    pub fn filter<P: FnOnce(&T) -> bool>(self, predicate: P) -> Self {
        if let COption::Some(x) = self {
            if predicate(&x) {
                return COption::Some(x);
            }
        }
        COption::None
    }

    /// Returns the option if it contains a value, otherwise returns `optb`.
    ///
    /// Arguments passed to `or` are eagerly evaluated; if you are passing the
    /// result of a function call, it is recommended to use [`or_else`], which is
    /// lazily evaluated.
    ///
    /// [`or_else`]: #method.or_else
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = COption::Some(2);
    /// let y = COption::None;
    /// assert_eq!(x.or(y), COption::Some(2));
    ///
    /// let x = COption::None;
    /// let y = COption::Some(100);
    /// assert_eq!(x.or(y), COption::Some(100));
    ///
    /// let x = COption::Some(2);
    /// let y = COption::Some(100);
    /// assert_eq!(x.or(y), COption::Some(2));
    ///
    /// let x: COption<u32> = COption::None;
    /// let y = COption::None;
    /// assert_eq!(x.or(y), COption::None);
    /// ```
    #[inline]
    pub fn or(self, optb: COption<T>) -> COption<T> {
        match self {
            COption::Some(_) => self,
            COption::None => optb,
        }
    }

    /// Returns the option if it contains a value, otherwise calls `f` and
    /// returns the result.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// fn nobody() -> COption<&'static str> { COption::None }
    /// fn vikings() -> COption<&'static str> { COption::Some("vikings") }
    ///
    /// assert_eq!(COption::Some("barbarians").or_else(vikings), COption::Some("barbarians"));
    /// assert_eq!(COption::None.or_else(vikings), COption::Some("vikings"));
    /// assert_eq!(COption::None.or_else(nobody), COption::None);
    /// ```
    #[inline]
    pub fn or_else<F: FnOnce() -> COption<T>>(self, f: F) -> COption<T> {
        match self {
            COption::Some(_) => self,
            COption::None => f(),
        }
    }

    /// Returns [`COption::Some`] if exactly one of `self`, `optb` is [`COption::Some`], otherwise returns [`COption::None`].
    ///
    /// [`COption::Some`]: #variant.COption::Some
    /// [`COption::None`]: #variant.COption::None
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = COption::Some(2);
    /// let y: COption<u32> = COption::None;
    /// assert_eq!(x.xor(y), COption::Some(2));
    ///
    /// let x: COption<u32> = COption::None;
    /// let y = COption::Some(2);
    /// assert_eq!(x.xor(y), COption::Some(2));
    ///
    /// let x = COption::Some(2);
    /// let y = COption::Some(2);
    /// assert_eq!(x.xor(y), COption::None);
    ///
    /// let x: COption<u32> = COption::None;
    /// let y: COption<u32> = COption::None;
    /// assert_eq!(x.xor(y), COption::None);
    /// ```
    #[inline]
    pub fn xor(self, optb: COption<T>) -> COption<T> {
        match (self, optb) {
            (COption::Some(a), COption::None) => COption::Some(a),
            (COption::None, COption::Some(b)) => COption::Some(b),
            _ => COption::None,
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // Entry-like operations to insert if COption::None and return a reference
    /////////////////////////////////////////////////////////////////////////

    /// Inserts `v` into the option if it is [`COption::None`], then
    /// returns a mutable reference to the contained value.
    ///
    /// [`COption::None`]: #variant.COption::None
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut x = COption::None;
    ///
    /// {
    ///     let y: &mut u32 = x.get_or_insert(5);
    ///     assert_eq!(y, &5);
    ///
    ///     *y = 7;
    /// }
    ///
    /// assert_eq!(x, COption::Some(7));
    /// ```
    #[inline]
    pub fn get_or_insert(&mut self, v: T) -> &mut T {
        self.get_or_insert_with(|| v)
    }

    /// Inserts a value computed from `f` into the option if it is [`COption::None`], then
    /// returns a mutable reference to the contained value.
    ///
    /// [`COption::None`]: #variant.COption::None
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut x = COption::None;
    ///
    /// {
    ///     let y: &mut u32 = x.get_or_insert_with(|| 5);
    ///     assert_eq!(y, &5);
    ///
    ///     *y = 7;
    /// }
    ///
    /// assert_eq!(x, COption::Some(7));
    /// ```
    #[inline]
    pub fn get_or_insert_with<F: FnOnce() -> T>(&mut self, f: F) -> &mut T {
        if let COption::None = *self {
            *self = COption::Some(f())
        }

        match *self {
            COption::Some(ref mut v) => v,
            COption::None => unreachable!(),
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // Misc
    /////////////////////////////////////////////////////////////////////////

    /// Replaces the actual value in the option by the value given in parameter,
    /// returning the old value if present,
    /// leaving a [`COption::Some`] in its place without deinitializing either one.
    ///
    /// [`COption::Some`]: #variant.COption::Some
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut x = COption::Some(2);
    /// let old = x.replace(5);
    /// assert_eq!(x, COption::Some(5));
    /// assert_eq!(old, COption::Some(2));
    ///
    /// let mut x = COption::None;
    /// let old = x.replace(3);
    /// assert_eq!(x, COption::Some(3));
    /// assert_eq!(old, COption::None);
    /// ```
    #[inline]
    pub fn replace(&mut self, value: T) -> COption<T> {
        mem::replace(self, COption::Some(value))
    }
}

impl<T: Copy> COption<&T> {
    /// Maps an `COption<&T>` to an `COption<T>` by copying the contents of the
    /// option.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = 12;
    /// let opt_x = COption::Some(&x);
    /// assert_eq!(opt_x, COption::Some(&12));
    /// let copied = opt_x.copied();
    /// assert_eq!(copied, COption::Some(12));
    /// ```
    pub fn copied(self) -> COption<T> {
        self.map(|&t| t)
    }
}

impl<T: Copy> COption<&mut T> {
    /// Maps an `COption<&mut T>` to an `COption<T>` by copying the contents of the
    /// option.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut x = 12;
    /// let opt_x = COption::Some(&mut x);
    /// assert_eq!(opt_x, COption::Some(&mut 12));
    /// let copied = opt_x.copied();
    /// assert_eq!(copied, COption::Some(12));
    /// ```
    pub fn copied(self) -> COption<T> {
        self.map(|&mut t| t)
    }
}

impl<T: Clone> COption<&T> {
    /// Maps an `COption<&T>` to an `COption<T>` by cloning the contents of the
    /// option.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let x = 12;
    /// let opt_x = COption::Some(&x);
    /// assert_eq!(opt_x, COption::Some(&12));
    /// let cloned = opt_x.cloned();
    /// assert_eq!(cloned, COption::Some(12));
    /// ```
    pub fn cloned(self) -> COption<T> {
        self.map(|t| t.clone())
    }
}

impl<T: Clone> COption<&mut T> {
    /// Maps an `COption<&mut T>` to an `COption<T>` by cloning the contents of the
    /// option.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut x = 12;
    /// let opt_x = COption::Some(&mut x);
    /// assert_eq!(opt_x, COption::Some(&mut 12));
    /// let cloned = opt_x.cloned();
    /// assert_eq!(cloned, COption::Some(12));
    /// ```
    pub fn cloned(self) -> COption<T> {
        self.map(|t| t.clone())
    }
}

impl<T: Default> COption<T> {
    /// Returns the contained value or a default
    ///
    /// Consumes the `self` argument then, if [`COption::Some`], returns the contained
    /// value, otherwise if [`COption::None`], returns the [default value] for that
    /// type.
    ///
    /// # Examples
    ///
    /// Converts a string to an integer, turning poorly-formed strings
    /// into 0 (the default value for integers). [`parse`] converts
    /// a string to any other type that implements [`FromStr`], returning
    /// [`COption::None`] on error.
    ///
    /// ```ignore
    /// let good_year_from_input = "1909";
    /// let bad_year_from_input = "190blarg";
    /// let good_year = good_year_from_input.parse().ok().unwrap_or_default();
    /// let bad_year = bad_year_from_input.parse().ok().unwrap_or_default();
    ///
    /// assert_eq!(1909, good_year);
    /// assert_eq!(0, bad_year);
    /// ```
    ///
    /// [`COption::Some`]: #variant.COption::Some
    /// [`COption::None`]: #variant.COption::None
    /// [default value]: ../default/trait.Default.html#tymethod.default
    /// [`parse`]: ../../std/primitive.str.html#method.parse
    /// [`FromStr`]: ../../std/str/trait.FromStr.html
    #[inline]
    pub fn unwrap_or_default(self) -> T {
        match self {
            COption::Some(x) => x,
            COption::None => T::default(),
        }
    }
}

impl<T: Deref> COption<T> {
    /// Converts from `COption<T>` (or `&COption<T>`) to `COption<&T::Target>`.
    ///
    /// Leaves the original COption in-place, creating a new one with a reference
    /// to the original one, additionally coercing the contents via [`Deref`].
    ///
    /// [`Deref`]: ../../std/ops/trait.Deref.html
    ///
    /// # Examples
    ///
    /// ```ignore
    /// #![feature(inner_deref)]
    ///
    /// let x: COption<String> = COption::Some("hey".to_owned());
    /// assert_eq!(x.as_deref(), COption::Some("hey"));
    ///
    /// let x: COption<String> = COption::None;
    /// assert_eq!(x.as_deref(), COption::None);
    /// ```
    pub fn as_deref(&self) -> COption<&T::Target> {
        self.as_ref().map(|t| t.deref())
    }
}

impl<T: DerefMut> COption<T> {
    /// Converts from `COption<T>` (or `&mut COption<T>`) to `COption<&mut T::Target>`.
    ///
    /// Leaves the original `COption` in-place, creating a new one containing a mutable reference to
    /// the inner type's `Deref::Target` type.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// #![feature(inner_deref)]
    ///
    /// let mut x: COption<String> = COption::Some("hey".to_owned());
    /// assert_eq!(x.as_deref_mut().map(|x| {
    ///     x.make_ascii_uppercase();
    ///     x
    /// }), COption::Some("HEY".to_owned().as_mut_str()));
    /// ```
    pub fn as_deref_mut(&mut self) -> COption<&mut T::Target> {
        self.as_mut().map(|t| t.deref_mut())
    }
}

impl<T, E> COption<Result<T, E>> {
    /// Transposes an `COption` of a [`Result`] into a [`Result`] of an `COption`.
    ///
    /// [`COption::None`] will be mapped to [`Ok`]`(`[`COption::None`]`)`.
    /// [`COption::Some`]`(`[`Ok`]`(_))` and [`COption::Some`]`(`[`Err`]`(_))` will be mapped to
    /// [`Ok`]`(`[`COption::Some`]`(_))` and [`Err`]`(_)`.
    ///
    /// [`COption::None`]: #variant.COption::None
    /// [`Ok`]: ../../std/result/enum.Result.html#variant.Ok
    /// [`COption::Some`]: #variant.COption::Some
    /// [`Err`]: ../../std/result/enum.Result.html#variant.Err
    ///
    /// # Examples
    ///
    /// ```ignore
    /// #[derive(Debug, Eq, PartialEq)]
    /// struct COption::SomeErr;
    ///
    /// let x: Result<COption<i32>, COption::SomeErr> = Ok(COption::Some(5));
    /// let y: COption<Result<i32, COption::SomeErr>> = COption::Some(Ok(5));
    /// assert_eq!(x, y.transpose());
    /// ```
    #[inline]
    pub fn transpose(self) -> Result<COption<T>, E> {
        match self {
            COption::Some(Ok(x)) => Ok(COption::Some(x)),
            COption::Some(Err(e)) => Err(e),
            COption::None => Ok(COption::None),
        }
    }
}

// This is a separate function to reduce the code size of .expect() itself.
#[inline(never)]
#[cold]
fn expect_failed(msg: &str) -> ! {
    panic!("{}", msg)
}

// // This is a separate function to reduce the code size of .expect_none() itself.
// #[inline(never)]
// #[cold]
// fn expect_none_failed(msg: &str, value: &dyn fmt::Debug) -> ! {
//     panic!("{}: {:?}", msg, value)
// }

/////////////////////////////////////////////////////////////////////////////
// Trait implementations
/////////////////////////////////////////////////////////////////////////////

impl<T: Clone> Clone for COption<T> {
    #[inline]
    fn clone(&self) -> Self {
        match self {
            COption::Some(x) => COption::Some(x.clone()),
            COption::None => COption::None,
        }
    }

    #[inline]
    fn clone_from(&mut self, source: &Self) {
        match (self, source) {
            (COption::Some(to), COption::Some(from)) => to.clone_from(from),
            (to, from) => *to = from.clone(),
        }
    }
}

impl<T> Default for COption<T> {
    /// Returns [`COption::None`][COption::None].
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let opt: COption<u32> = COption::default();
    /// assert!(opt.is_none());
    /// ```
    #[inline]
    fn default() -> COption<T> {
        COption::None
    }
}

impl<T> From<T> for COption<T> {
    fn from(val: T) -> COption<T> {
        COption::Some(val)
    }
}

impl<'a, T> From<&'a COption<T>> for COption<&'a T> {
    fn from(o: &'a COption<T>) -> COption<&'a T> {
        o.as_ref()
    }
}

impl<'a, T> From<&'a mut COption<T>> for COption<&'a mut T> {
    fn from(o: &'a mut COption<T>) -> COption<&'a mut T> {
        o.as_mut()
    }
}

impl<T> COption<COption<T>> {
    /// Converts from `COption<COption<T>>` to `COption<T>`
    ///
    /// # Examples
    /// Basic usage:
    /// ```ignore
    /// #![feature(option_flattening)]
    /// let x: COption<COption<u32>> = COption::Some(COption::Some(6));
    /// assert_eq!(COption::Some(6), x.flatten());
    ///
    /// let x: COption<COption<u32>> = COption::Some(COption::None);
    /// assert_eq!(COption::None, x.flatten());
    ///
    /// let x: COption<COption<u32>> = COption::None;
    /// assert_eq!(COption::None, x.flatten());
    /// ```
    /// Flattening once only removes one level of nesting:
    /// ```ignore
    /// #![feature(option_flattening)]
    /// let x: COption<COption<COption<u32>>> = COption::Some(COption::Some(COption::Some(6)));
    /// assert_eq!(COption::Some(COption::Some(6)), x.flatten());
    /// assert_eq!(COption::Some(6), x.flatten().flatten());
    /// ```
    #[inline]
    pub fn flatten(self) -> COption<T> {
        self.and_then(convert::identity)
    }
}

impl<T> From<Option<T>> for COption<T> {
    fn from(option: Option<T>) -> Self {
        match option {
            Some(value) => COption::Some(value),
            None => COption::None,
        }
    }
}

#[rustversion::since(1.49.0)]
impl<T> From<COption<T>> for Option<T> {
    fn from(coption: COption<T>) -> Self {
        match coption {
            COption::Some(value) => Some(value),
            COption::None => None,
        }
    }
}

#[rustversion::before(1.49.0)] // Remove `Into` once the BPF toolchain upgrades to 1.49.0 or newer
impl<T> Into<Option<T>> for COption<T> {
    fn into(self) -> Option<T> {
        match self {
            COption::Some(value) => Some(value),
            COption::None => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_from_rust_option() {
        let option = Some(99u64);
        let c_option: COption<u64> = option.into();
        assert_eq!(c_option, COption::Some(99u64));
        let expected = c_option.into();
        assert_eq!(option, expected);

        let option = None;
        let c_option: COption<u64> = option.into();
        assert_eq!(c_option, COption::None);
        let expected = c_option.into();
        assert_eq!(option, expected);
    }
}
