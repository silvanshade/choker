#[cfg_attr(feature = "debug", derive(Debug))]
struct Blake3Key([u8; 32]);

// const TABLE: redb::TableDefinition<Blake3Key, &[u8]> = redb::TableDefinition::new("my_data");

impl From<blake3::Hash> for Blake3Key {
    fn from(hash: blake3::Hash) -> Self {
        Self(hash.into())
    }
}

impl redb::RedbKey for Blake3Key {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

impl redb::RedbValue for Blake3Key {
    type SelfType<'a> = Self
    where
        Self: 'a;

    type AsBytes<'a> = [u8; 32]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(32)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let array = <[u8; 32]>::try_from(data).unwrap();
        let hash = blake3::Hash::from_bytes(array).into();
        Self(hash)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        value.0
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("Blake3Key")
    }
}
