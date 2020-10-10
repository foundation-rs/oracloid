use std::{
    error, fmt, ptr
};

/// Represents Oracle error
#[derive(Debug, Clone)]
pub struct OracleError {
    /// Oracle error code
    pub errcode: i32,
    /// Message from Oracle
    message:     String,
    // Function where error occured
    location:    &'static str
}

pub type OracleResult<T> = Result<T, OracleError>;

impl OracleError {
    pub fn new(message: String, location: &'static str) -> OracleError {
        OracleError { errcode: 200, message, location}
    }
}

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!{f, "\n\n   Error code: {}\n   Error message: {}\n   Where: {}\n\n",
                self.errcode, self.message, self.location}
    }
}

impl error::Error for OracleError {
    fn description(&self) -> &str {
        self.message.as_str()
    }
}

// TODO: create custom OracleError

/// Returns an error message in the buffer provided and an ORACLE error
#[inline]
fn error_get(errhp: *mut OCIError, location: &'static str) -> OracleError {
    let errc: *mut i32 = &mut 0;
    let mut buf = String::with_capacity(2048);
    unsafe {
        OCIErrorGet(
            errhp as *mut c_void, // hndlp
            1,                    // recordno
            ptr::null_mut(),      // sqlstate
            errc,                 // errcodep
            buf.as_mut_ptr() as *mut u8,  // bufp
            buf.capacity() as u32,        // bufsiz
            OCI_HTYPE_ERROR
        )
    };
    OracleError { errcode: unsafe{ *errc }, message: buf, location }
}

/// check errcode for Oracle Error
pub fn check_error(errcode: i32,
                   handle: Option<*mut OCIError>,
                   location: &'static str) -> Result<(), OracleError> {
    if errcode == OCI_SUCCESS {
        Ok(())
    } else {
        let by_handle =
            handle.map(|errhp| {
                let mut error = error_get(errhp, location);
                if error.errcode == 24347 {
                    error.message = "NULL column in a aggregate function".to_string();
                }
                error
            });

        let oracleerr =
            if errcode == OCI_ERROR {
                by_handle.unwrap_or(
                    OracleError { errcode, message: "Error with no details".to_string(), location }
                )
            } else if errcode == OCI_SUCCESS_WITH_INFO {
                by_handle.unwrap_or(
                    OracleError { errcode, message: "Success with info".to_string(), location }
                )

            } else {
                let message =
                    match errcode {
                        OCI_NO_DATA => "No data",
                        OCI_INVALID_HANDLE => "Invalid handle",
                        OCI_NEED_DATA => "Need data",
                        OCI_STILL_EXECUTING => "Steel executing",
                        _ => panic!("Unknow return code")
                    }.to_string();
                OracleError { errcode, message, location }
            };
            Err(oracleerr)
        }
}

