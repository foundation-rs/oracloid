#[macro_use]
use lazy_static::lazy_static;

use super::oci;
use super::statement;
use super::values::{DescriptorsProvider, FromResultSet};

/// Oracle environment
struct Environment {
    envhp: *mut oci::OCIEnv,
    errhp: *mut oci::OCIError
}

/// Connection to Oracle and server context
pub struct Connection {
    env: &'static Environment,
    srvhp: *mut oci::OCIServer,
    authp: *mut oci::OCISession,
    pub(crate) errhp: *mut oci::OCIError,
    pub(crate) svchp: *mut oci::OCISvcCtx,
}

type EnvironmentResult = Result<Environment, oci::OracleError>;

// for multithreading and lazy_static
unsafe impl Sync for Environment {}
unsafe impl Send for Environment {}

lazy_static! {
  static ref ORACLE_ENV: EnvironmentResult = Environment::new();
}

impl Environment {

    /// Create new environment
    fn new() -> Result<Environment, oci::OracleError> {
        let envhp = oci::env_create()?;
        // create error handle
        let errhp = oci::handle_alloc(envhp, oci::OCI_HTYPE_ERROR)? as *mut oci::OCIError;
        Ok(Environment{ envhp, errhp })
    }

    fn get() -> Result<&'static Environment, oci::OracleError> {
        match *ORACLE_ENV {
            Ok(ref env) => Ok(env),
            Err(ref err) => Err(err.to_owned())
        }
    }

}

impl Drop for Environment {
    fn drop(&mut self) {
        oci::handle_free(self.errhp as *mut oci::c_void, oci::OCI_HTYPE_ERROR);
        oci::handle_free(self.envhp as *mut oci::c_void, oci::OCI_HTYPE_ENV);
        oci::terminate();
    }
}

/// connect to database
pub fn connect(db: &str, username: &str, passwd: &str) -> Result<Connection, oci::OracleError> {
    let env = Environment::get()?;
    let srvhp = oci::handle_alloc(env.envhp, oci::OCI_HTYPE_SERVER)? as *mut oci::OCIServer;
    let svchp = oci::handle_alloc(env.envhp, oci::OCI_HTYPE_SVCCTX)? as *mut oci::OCISvcCtx;

    let errhp = env.errhp;
    let res = oci::server_attach(srvhp, errhp, db);
    if let Err(err) = res {
        free_server_handlers(srvhp, svchp);
        return Err(err);
    };

    // set attribute server context in the service context
    oci::attr_set(svchp as *mut oci::c_void,
                  oci::OCI_HTYPE_SVCCTX,
                  srvhp as *mut oci::c_void,
                  0,
                  oci::OCI_ATTR_SERVER,
                  errhp)?;

    let authp = oci::prepare_auth(env.envhp, errhp, username, passwd)?;

    let res = oci::session_begin(svchp, errhp, authp);
    if let Err(err) = res {
        free_session_handler(authp);
        free_server_handlers(srvhp, svchp);
        return Err(err);
    };

    // set session context in the service context
    oci::attr_set(svchp as *mut oci::c_void, oci::OCI_HTYPE_SVCCTX,
                  authp as *mut oci::c_void, 0,
                  oci::OCI_ATTR_SESSION, errhp)?;


    return Ok( Connection::new(env, srvhp, authp, errhp, svchp ) );
}

impl Connection {
    fn new(env: &'static Environment,
           srvhp: *mut oci::OCIServer,
           authp: *mut oci::OCISession,
           errhp: *mut oci::OCIError,
           svchp: *mut oci::OCISvcCtx) -> Connection {
        Connection { env, srvhp, authp, errhp, svchp }
    }

    /// commit transaction with NO-WAIT option
    pub fn commit(&self) -> Result<(), oci::OracleError> {
        oci::commit(self.svchp, self.env.errhp)
    }

    /// rollback transation
    pub fn rollback(&self) -> Result<(), oci::OracleError> {
        oci::rollback(self.svchp, self.env.errhp)
    }

    // TODO: row prefetch size
    /// Prepare oracle statement
    pub fn make_query<'conn,'s,R: DescriptorsProvider + FromResultSet>(&'conn self, sql: &'s str) -> Result<statement::Query<'conn,R>, oci::OracleError> {
        statement::Query::new(self, sql)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        oci::session_end(self.svchp, self.env.errhp, self.authp);
        oci::server_detach(self.srvhp, self.env.errhp);
        free_session_handler(self.authp);
        free_server_handlers(self.srvhp, self.svchp);
    }
}

fn free_session_handler(authp: *mut oci::OCISession) {
    if !authp.is_null() {
        oci::handle_free(authp as *mut oci::c_void, oci::OCI_HTYPE_SESSION);
    }
}

fn free_server_handlers(srvhp: *mut oci::OCIServer, svchp: *mut oci::OCISvcCtx) {
    if !svchp.is_null() {
        oci::handle_free(svchp as *mut oci::c_void, oci::OCI_HTYPE_SVCCTX);
    }
    if !srvhp.is_null() {
        oci::handle_free(srvhp as *mut oci::c_void, oci::OCI_HTYPE_SERVER);
    }
}

