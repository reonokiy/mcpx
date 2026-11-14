//! A single pass into serde_json::Value, preserving its number representation.
//! The wrapper rejects non-finite float events before serde_json turns them into
//! null. Compound children must keep the wrapper, including map keys and enums.
use crate::{Error, Result};
use serde::{Serialize, Serializer, ser};
use std::cell::Cell;

pub const MAX_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_SERDE_DEPTH: usize = 128;

struct Checked<'a, T: ?Sized> {
    value: &'a T,
    depth: usize,
    failure: &'a Cell<Option<&'static str>>,
}
impl<T: Serialize + ?Sized> Serialize for Checked<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        if self.depth > MAX_SERDE_DEPTH {
            self.failure.set(Some("LimitExceeded"));
            return Err(ser::Error::custom("mcpx serialization depth"));
        }
        self.value.serialize(Finite {
            inner: serializer,
            depth: self.depth,
            failure: self.failure,
        })
    }
}
struct Finite<'a, S> {
    inner: S,
    depth: usize,
    failure: &'a Cell<Option<&'static str>>,
}
struct Compound<'a, C> {
    inner: C,
    depth: usize,
    failure: &'a Cell<Option<&'static str>>,
}
macro_rules! scalar {
    ($($name:ident($ty:ty)),* $(,)?)=>{$(
        fn $name(self,value:$ty)->std::result::Result<Self::Ok,Self::Error> {self.inner.$name(value)}
    )*};
}
impl<'a, S: Serializer> Serializer for Finite<'a, S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = Compound<'a, S::SerializeSeq>;
    type SerializeTuple = Compound<'a, S::SerializeTuple>;
    type SerializeTupleStruct = Compound<'a, S::SerializeTupleStruct>;
    type SerializeTupleVariant = Compound<'a, S::SerializeTupleVariant>;
    type SerializeMap = Compound<'a, S::SerializeMap>;
    type SerializeStruct = Compound<'a, S::SerializeStruct>;
    type SerializeStructVariant = Compound<'a, S::SerializeStructVariant>;
    scalar!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8])
    );
    fn serialize_f32(self, value: f32) -> std::result::Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            self.failure.set(Some("NumberOutOfRange"));
            return Err(ser::Error::custom("mcpx non-finite number"));
        }
        self.inner.serialize_f32(value)
    }
    fn serialize_f64(self, value: f64) -> std::result::Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            self.failure.set(Some("NumberOutOfRange"));
            return Err(ser::Error::custom("mcpx non-finite number"));
        }
        self.inner.serialize_f64(value)
    }
    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        self.inner.serialize_none()
    }
    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> {
        self.inner.serialize_unit()
    }
    fn serialize_unit_struct(
        self,
        name: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        self.inner.serialize_unit_struct(name)
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        self.inner.serialize_unit_variant(name, index, variant)
    }
    fn serialize_some<T: Serialize + ?Sized>(
        self,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        self.inner.serialize_some(&Checked {
            value,
            depth: self.depth + 1,
            failure: self.failure,
        })
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        self.inner.serialize_newtype_struct(
            name,
            &Checked {
                value,
                depth: self.depth + 1,
                failure: self.failure,
            },
        )
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        self.inner.serialize_newtype_variant(
            name,
            index,
            variant,
            &Checked {
                value,
                depth: self.depth + 1,
                failure: self.failure,
            },
        )
    }
    fn serialize_seq(
        self,
        _length: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, Self::Error> {
        Ok(Compound {
            inner: self.inner.serialize_seq(None)?,
            depth: self.depth,
            failure: self.failure,
        })
    }
    fn serialize_tuple(
        self,
        length: usize,
    ) -> std::result::Result<Self::SerializeTuple, Self::Error> {
        Ok(Compound {
            inner: self.inner.serialize_tuple(length.min(64))?,
            depth: self.depth,
            failure: self.failure,
        })
    }
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        length: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(Compound {
            inner: self.inner.serialize_tuple_struct(name, length.min(64))?,
            depth: self.depth,
            failure: self.failure,
        })
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        length: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(Compound {
            inner: self
                .inner
                .serialize_tuple_variant(name, index, variant, length.min(64))?,
            depth: self.depth,
            failure: self.failure,
        })
    }
    fn serialize_map(
        self,
        _length: Option<usize>,
    ) -> std::result::Result<Self::SerializeMap, Self::Error> {
        Ok(Compound {
            inner: self.inner.serialize_map(None)?,
            depth: self.depth,
            failure: self.failure,
        })
    }
    fn serialize_struct(
        self,
        name: &'static str,
        length: usize,
    ) -> std::result::Result<Self::SerializeStruct, Self::Error> {
        Ok(Compound {
            inner: self.inner.serialize_struct(name, length.min(64))?,
            depth: self.depth,
            failure: self.failure,
        })
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        index: u32,
        variant: &'static str,
        length: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, Self::Error> {
        Ok(Compound {
            inner: self
                .inner
                .serialize_struct_variant(name, index, variant, length.min(64))?,
            depth: self.depth,
            failure: self.failure,
        })
    }
    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }
}
macro_rules! sequence {
    ($trait:ident,$method:ident) => {
        impl<C: ser::$trait> ser::$trait for Compound<'_, C> {
            type Ok = C::Ok;
            type Error = C::Error;
            fn $method<T: Serialize + ?Sized>(
                &mut self,
                value: &T,
            ) -> std::result::Result<(), Self::Error> {
                self.inner.$method(&Checked {
                    value,
                    depth: self.depth + 1,
                    failure: self.failure,
                })
            }
            fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
                self.inner.end()
            }
        }
    };
}
sequence!(SerializeSeq, serialize_element);
sequence!(SerializeTuple, serialize_element);
sequence!(SerializeTupleStruct, serialize_field);
sequence!(SerializeTupleVariant, serialize_field);
impl<C: ser::SerializeMap> ser::SerializeMap for Compound<'_, C> {
    type Ok = C::Ok;
    type Error = C::Error;
    fn serialize_key<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        self.inner.serialize_key(&Checked {
            value,
            depth: self.depth + 1,
            failure: self.failure,
        })
    }
    fn serialize_value<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        self.inner.serialize_value(&Checked {
            value,
            depth: self.depth + 1,
            failure: self.failure,
        })
    }
    fn serialize_entry<K: Serialize + ?Sized, V: Serialize + ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> std::result::Result<(), Self::Error> {
        self.inner.serialize_entry(
            &Checked {
                value: key,
                depth: self.depth + 1,
                failure: self.failure,
            },
            &Checked {
                value,
                depth: self.depth + 1,
                failure: self.failure,
            },
        )
    }
    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        self.inner.end()
    }
}
macro_rules! structure {
    ($trait:ident) => {
        impl<C: ser::$trait> ser::$trait for Compound<'_, C> {
            type Ok = C::Ok;
            type Error = C::Error;
            fn serialize_field<T: Serialize + ?Sized>(
                &mut self,
                key: &'static str,
                value: &T,
            ) -> std::result::Result<(), Self::Error> {
                self.inner.serialize_field(
                    key,
                    &Checked {
                        value,
                        depth: self.depth + 1,
                        failure: self.failure,
                    },
                )
            }
            fn skip_field(&mut self, key: &'static str) -> std::result::Result<(), Self::Error> {
                self.inner.skip_field(key)
            }
            fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
                self.inner.end()
            }
        }
    };
}
structure!(SerializeStruct);
structure!(SerializeStructVariant);

pub(crate) fn serialize_value(value: impl Serialize) -> Result<serde_json::Value> {
    let failure = Cell::new(None);
    let result = serde_json::to_value(Checked {
        value: &value,
        depth: 0,
        failure: &failure,
    });
    if let Some(code) = failure.get() {
        return Err(Error::local(
            code,
            if code == "NumberOutOfRange" {
                "non-finite numbers are not supported"
            } else {
                "serialization nesting exceeds 128 levels"
            },
        ));
    }
    let result = result
        .map_err(|_| Error::local("SerializationFailed", "cannot represent argument as JSON"))?;
    crate::check_json(&result, 0)?;
    check_size(&result)?;
    Ok(result)
}
pub(crate) fn check_size(value: &serde_json::Value) -> Result<()> {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            if self.0 > MAX_ARGUMENT_BYTES {
                return Err(std::io::Error::other("argument size"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Count(0), value)
        .map_err(|_| Error::local("LimitExceeded", "arguments exceed 1 MiB"))
}
