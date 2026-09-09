use steamworks::{SteamId, networking_types::NetworkingIdentity};

#[derive(Clone, Debug)]
pub struct ConnectInfo {
    pub identity: NetworkingIdentity
}

impl ConnectInfo {
    pub fn new(steam_id: SteamId) -> Self {
        Self {
            identity: NetworkingIdentity::new_steam_id(steam_id)
        }
    }
}

impl<'a> TryFrom<&'a [u8]> for ConnectInfo {
    type Error = ();

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        let Some(array) = value.as_array::<8>() else {
            // if not exactly size 8, the result is unexpected
            return Err(());
        };
        let raw_steam_id = u64::from_be_bytes(*array);
        let steam_id = SteamId::from_raw(raw_steam_id);
        Ok(Self {
            identity: NetworkingIdentity::new_steam_id(steam_id)
        })
    }
}

impl TryInto<Vec<u8>> for ConnectInfo {
    type Error = ();

    fn try_into(self) -> Result<Vec<u8>, ()> {
        // TODO: encode other networking identites other than steam id
        // for now everything that is not a steam id will be an error
        let Some(steam_id) = self.identity.steam_id() else {
            return Err(());
        };
        let mut b = vec![0u8; 8];
        let steam_id_bytes = steam_id.raw().to_be_bytes();
        if let Some(array) = b.as_mut_slice().as_mut_array::<8>() {
            *array = steam_id_bytes;
        };
        Ok(b)
    }
}