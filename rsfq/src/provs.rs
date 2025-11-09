pub mod ena;
pub mod sra;

/// Enum representing the providers
#[derive(Debug, Clone, Copy)]
pub enum Provider {
    ENA,
    SRA,
}

/// Parse a string into a Provider
impl std::str::FromStr for Provider {
    type Err = String;

    /// Parse a string into a Provider
    ///
    /// # Arguments
    /// * `s` - The string to parse.
    ///
    /// # Returns
    /// * `Result<Self, Self::Err>` - The parsed Provider.
    ///
    /// # Examples
    /// ```rust, no_run
    /// use rsfq::provs::Provider;
    /// use std::str::FromStr;
    /// let provider = Provider::from_str("ena");
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ena" => Ok(Provider::ENA),
            "sra" => Ok(Provider::SRA),
            _ => Err(format!("Invalid provider: {}", s)),
        }
    }
}

impl std::fmt::Display for Provider {
    /// Format the `Provider` instance as a string.
    ///
    /// # Arguments
    /// * `f` - The formatter to use.
    ///
    /// # Returns
    /// * `std::fmt::Result` - The formatted string.
    ///
    /// # Examples
    /// ```rust, no_run
    /// use rsfq::provs::Provider;
    /// use std::str::FromStr;
    /// let provider = Provider::from_str("ena").unwrap();
    /// println!("{}", provider);
    /// ```
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Provider::ENA => write!(f, "ena"),
            Provider::SRA => write!(f, "sra"),
        }
    }
}
