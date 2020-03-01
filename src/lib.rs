//! Runs ``diesel`` tests efficiently using a live Postgres connection.
//!
//! This test runner takes advantage of the highly transactional nature of Postgres to
//! quickly restore the database to a well-known state before each test.
//! Once the database schema is initialized, it can take only a fraction of a second
//! to run many tests.
//!
//! Example
//! -------
//!
//! This example test module starts with an empty database. The ``insert_and_query_user()``
//! test uses a global variable called ``PGTEST`` to run code in a live Postgres
//! transaction. The ``init_db()`` function could read the schema from a file or use Diesel
//! migration to initialize the schema.
//!
//! ```rust
//! #[macro_use] extern crate diesel;
//! use diesel::connection::SimpleConnection;
//! use diesel::prelude::*;
//! use diesel::sql_types::*;
//! use diesel_pg_tester::DieselPgTester;
//! use once_cell::sync::Lazy;
//!
//! // TestResult is the type that tests and init_db() return. Any error type is acceptable, but
//! // diesel::result::Error is convenient for this example.
//! type TestResult<T = ()> = Result<T, diesel::result::Error>;
//!
//! // PGTEST is the global DieselPgTester that the whole test suite will use.
//! // The error type argument must match the TestResult error type.
//! static PGTEST: Lazy<DieselPgTester<diesel::result::Error>> =
//!     Lazy::new(|| DieselPgTester::start(1, None, init_db));
//!
//! // init_db() initializes the temporary schema in the database.
//! fn init_db(conn: &PgConnection) -> TestResult {
//!     conn.batch_execute(
//!         "CREATE TABLE users (id BIGSERIAL PRIMARY KEY NOT NULL, name VARCHAR);")?;
//!     Ok(())
//! }
//!
//! // This table! macro invocation generates a helpful submodule called users::dsl.
//! table! {
//!     users (id) {
//!         id -> Int8,
//!         name -> Varchar,
//!     }
//! }
//!
//! // insert_and_query_user() is a sample test that uses PGTEST and the users::dsl submodule.
//! #[test]
//! fn insert_and_query_user() -> TestResult {
//!     PGTEST.run(|conn| {
//!         use users::dsl as U;
//!         diesel::insert_into(U::users).values(&U::name.eq("Quentin")).execute(conn)?;
//!         let user: (i64, String) = U::users.first(conn)?;
//!         assert_eq!("Quentin", user.1);
//!         Ok(())
//!     })
//! }
//!
//! # // Actually run that test!
//! # fn insert_and_query_user_real() -> TestResult {
//! #     PGTEST.run(|conn| {
//! #         use users::dsl as U;
//! #         diesel::insert_into(U::users).values(&U::name.eq("Quentin")).execute(conn)?;
//! #         let user: (i64, String) = U::users.first(conn)?;
//! #         assert_eq!("Quentin", user.1);
//! #         Ok(())
//! #     })
//! # }
//! # insert_and_query_user_real().unwrap();
//! ```
//!
//! How It Works
//! ------------
//!
//! Tests are run as follows:
//!
//! - A test suite creates a global ``DieselPgTester``, which starts one or more worker threads.
//!
//! - The test suite adds tests to the ``DieselPgTester``'s queue.
//!
//! - Each worker thread opens a connection to Postgres and starts a transaction.
//!
//! - Inside the transaction, the worker creates a temporary schema and runs an initialization
//!   function provided by the test suite to create the tables and other objects needed by
//!   the code to be tested.
//!
//! - Once the schema is set up, the worker polls the queue for a test to run.
//!
//! - The worker creates a savepoint.
//!
//! - The worker runs a test, reporting the test result back to the thread that queued the test.
//!
//! - After the test, the worker restores the savepoint created before the test, which reverts the
//!   schema and data to its state before the test.
//!
//! - The worker repeatedly polls the queue and runs tests until
//!   the process ends, a test panics, or the ``DieselPgTester`` is dropped.
//!
//! - Because the transaction is never committed, the database is left in its original state for
//!   later test runs.
//!
//! During development, tests often panic. When a test running in ``DieselPgTester`` panics,
//! ``DieselPgTester`` chooses the safe route: it drops the database connection,
//! the transaction is aborted, and the worker detects the panic and starts
//! another worker to replace itself. If there is only one worker, there will be a pause
//! after every panicked test as a new worker connects to the database and re-initializes.
//! To avoid this pause, tests can choose to return ``Result::Err`` rather than panic, allowing the
//! worker to clean up normally and quickly rather than drop the connection.
//!
//! If the database initialization fails or causes a panic, the ``DieselPgTester`` is halted to
//! avoid wasting time running tests that will ultimately fail. All tests running through a halted
//! ``DieselPgTester`` fail quickly.
//!
//! Usage Notes
//! -----------
//!
//! - Nothing should ever commit the transaction during testing. It is possible
//!   to commit the transaction using a manually emitted ``COMMIT`` statement, but doing so
//!   will likely make the test suite unreliable.
//!
//! - Even in Postgres, some database state (such as sequence values) is intentionally
//!   non-transactional. Avoid making your tests dependent on non-transactional state.
//!
//! - If the database schema depends on Postgres extensions or other features that must be
//!   set up by a database superuser, those features need to be set up before running tests.
//!
//! - The default Postgres schema is called ``public``, but ``DieselPgTester`` creates and
//!   uses a temporary schema rather than the ``public`` schema, so most SQL should not
//!   specify a schema name. Diesel doesn't normally generate SQL with schema names, but
//!   the ``pg_dump`` utility does, so you should strip out the ``public.`` prefix
//!   from SQL generated by ``pg_dump`` (except when using Postgres extensions that
//!   depend on the ``public`` schema.)

use chrono::offset::Utc;
use diesel::connection::SimpleConnection;
use diesel::result::Error;
use diesel::{Connection, PgConnection};
use std::collections::VecDeque;
use std::env;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use thread_id;

/// FnBox is a trick to avoid a borrow error when using FnOnce.
trait FnBox<E> {
    fn run(self: Box<Self>, conn: Rc<PgConnection>) -> Result<(), E>;
}

impl<F, E> FnBox<E> for F
where
    F: FnOnce(Rc<PgConnection>) -> Result<(), E>,
{
    fn run(self: Box<F>, conn: Rc<PgConnection>) -> Result<(), E> {
        (*self)(conn)
    }
}

type Job<E> = Box<dyn FnBox<E> + Send + 'static>;
type JobTuple<E> = (Job<E>, mpsc::Sender<Result<(), E>>);

/// Diesel test runner.
///
/// Create a global ``DieselPgTester`` for your tests (normally using ``once_cell::sync::Lazy``)
/// and use that global to run the body of each test.
pub struct DieselPgTester<E: Debug + Send + 'static> {
    /// All the DieselPgTester's state is in an Arc so we can use the state in any thread.
    runner: Arc<Runner<E>>,
}

impl<E: Debug + Send + 'static> DieselPgTester<E> {
    /// Creates a DieselPgTester and starts worker threads.
    ///
    /// Specify:
    ///
    /// - The number of worker threads to spawn. 1 or a small number is usually sufficient.
    ///   Running multiple threads isn't always faster than running a single thread.
    ///
    /// - The Postgres database URL, or use ``None`` to get the URL from the ``DATABASE_URL``
    ///   environment variable, defaulting to the string ``postgres:///``, which connects
    ///   to the user's default personal database in environments like Ubuntu.
    ///
    /// - The function to execute to initialize the database. Note that the initialization
    ///   function must not commit the transaction and that no tests will execute until
    ///   the initialization function completes successfully.
    pub fn start<I>(threads: usize, url: Option<String>, init_func: I) -> DieselPgTester<E>
    where
        I: Fn(&PgConnection) -> Result<(), E> + Send + Sync + 'static,
    {
        Self::start_rc(threads, url, move |conn_rc| init_func(&*conn_rc))
    }

    /// Creates a DieselPgTester and starts worker threads.
    ///
    /// This is like ``start()``, except the type of the connection passed to ``init_func`` is
    /// ``Rc<PgConnection>`` instead of ``&PgConnection``.
    pub fn start_rc<I>(threads: usize, url: Option<String>, init_func: I) -> DieselPgTester<E>
    where
        I: Fn(Rc<PgConnection>) -> Result<(), E> + Send + Sync + 'static,
    {
        assert!(threads > 0);

        let url = url.unwrap_or_else(|| {
            env::var("DATABASE_URL").unwrap_or_else(|_| "postgres:///".to_string())
        });
        let tester = DieselPgTester {
            runner: Arc::new(Runner {
                init_func: Box::new(init_func),
                url,
                task_queue: TaskQueue::default(),
            }),
        };

        for _ in 0..threads {
            Arc::clone(&tester.runner).spawn_worker();
        }

        tester
    }

    /// Runs a test that requires ``&PgConnection``.
    ///
    /// The test will be added to a queue and executed in a thread.
    pub fn run<F>(&self, f: F) -> Result<(), E>
    where
        F: FnOnce(&PgConnection) -> Result<(), E> + Send + 'static,
    {
        self.runner.run_job(Box::new(|c: Rc<PgConnection>| f(&*c)))
    }

    /// Runs a test that requires ``Rc<PgConnection>``.
    pub fn run_rc<F>(&self, f: F) -> Result<(), E>
    where
        F: FnOnce(Rc<PgConnection>) -> Result<(), E> + Send + 'static,
    {
        self.runner.run_job(Box::new(f))
    }

    /// Halts all workers and tests.
    pub fn halt(&self) {
        self.runner.halt();
    }
}

impl<E: Debug + Send + 'static> Drop for DieselPgTester<E> {
    fn drop(&mut self) {
        self.halt();
    }
}

struct Runner<E: Debug + Send + 'static> {
    init_func: Box<dyn Fn(Rc<PgConnection>) -> Result<(), E> + Send + Sync + 'static>,
    url: String,
    task_queue: TaskQueue<JobTuple<E>>,
}

impl<E: Debug + Send + 'static> Runner<E> {
    /// Starts a worker.
    fn spawn_worker(self: Arc<Self>) {
        let builder = thread::Builder::new().name("DieselPgTester".to_string());
        builder
            .spawn(move || self.run_worker())
            .expect("failed to spawn DieselPgTester worker");
    }

    /// run_worker() executes in a spawned thread.
    fn run_worker(self: Arc<Self>) {
        let conn = match PgConnection::establish(&self.url) {
            Err(e) => {
                self.halt();
                panic!("error connecting to {}: {}", self.url, e);
            }
            Ok(c) => Rc::new(c),
        };

        conn.test_transaction::<(), Error, _>(|| {
            // PanicHandler detects panics and reacts differently based on the worker's phase.
            let mut ph = PanicHandler {
                runner: Arc::clone(&self),
                phase: WorkerPhase::Init,
            };

            // Include the time stamp in the schema name so that if a transaction gets
            // accidentally committed by a test, it's easy to see when it happened.
            // Also include the process ID and thread ID to ensure uniqueness.
            let now = Utc::now();
            let schema_name = format!(
                "DieselPgTester_{}_{}_{}",
                now.format("%Y%m%dT%H%M%S"),
                std::process::id(),
                thread_id::get()
            );

            // println!("creating schema {}", schema_name);
            let schema_setup = format!(
                "CREATE SCHEMA {}; SET search_path TO {};",
                schema_name, schema_name
            );
            conn.batch_execute(&schema_setup)
                .expect("failed to create schema");
            (self.init_func)(Rc::clone(&conn)).expect("failed to initialize database");
            // println!("created schema {}", schema_name);

            loop {
                match self.task_queue.pop() {
                    Ok((job, result_sender)) => {
                        conn.transaction::<(), _, _>(|| {
                            ph.phase = WorkerPhase::Test;
                            let result = job.run(Rc::clone(&conn));
                            result_sender
                                .send(result)
                                .expect("unable to send test result");
                            Err(Error::RollbackTransaction)
                        })
                        .ok();
                    }
                    Err(_) => {
                        // The tester has halted.
                        ph.phase = WorkerPhase::Halt;
                        break Err(Error::RollbackTransaction);
                    }
                }
            }
        });
    }

    /// Executes a boxed test in a thread.
    fn run_job(&self, job: Job<E>) -> Result<(), E> {
        let (result_sender, result_receiver) = mpsc::channel();

        if self.task_queue.push((job, result_sender)).is_err() {
            self.report_halted();
        }

        result_receiver.recv().unwrap_or_else(|e| {
            if self.task_queue.is_halted() {
                // No test result received because the tester halted
                self.report_halted();
            } else {
                // No test result received for some other reason (usually because a test panicked)
                panic!("no test result received: {:?}", e);
            }
        })
    }

    fn report_halted(&self) -> ! {
        panic!("DieselPgTester halted (possibly due to an earlier initialization error)");
    }

    /// Halts all workers and tests.
    ///
    /// Returns true if the attempt to halt was successful. If it was not successful,
    /// a panic has already occurred; an error will be reported on stderr.
    fn halt(&self) -> bool {
        self.task_queue.halt()
    }
}

/// Halted is an internal Err type that indicates the work has halted.
struct Halted;

/// TaskQueue: a simple multithreaded queue that can be put in a halted state.
struct TaskQueue<T> {
    /// mutex: (halted, queue contents)
    mutex: Mutex<(bool, VecDeque<T>)>,
    cond: Condvar,
}

impl<T> Default for TaskQueue<T> {
    fn default() -> Self {
        Self {
            mutex: Mutex::new((false, VecDeque::new())),
            cond: Condvar::new(),
        }
    }
}

impl<T> TaskQueue<T> {
    fn push(&self, task: T) -> Result<(), Halted> {
        let mut hq = self.mutex.lock().unwrap();
        if hq.0 {
            Err(Halted)
        } else {
            hq.1.push_back(task);
            self.cond.notify_one();
            Ok(())
        }
    }

    fn pop(&self) -> Result<T, Halted> {
        let mut hq = self.mutex.lock().unwrap();
        while !hq.0 && hq.1.is_empty() {
            hq = self.cond.wait(hq).unwrap();
        }
        if hq.0 {
            return Err(Halted);
        }
        Ok(hq.1.pop_front().unwrap())
    }

    fn is_halted(&self) -> bool {
        self.mutex.lock().unwrap().0
    }

    fn halt(&self) -> bool {
        match self.mutex.lock() {
            Ok(mut hq) => {
                hq.0 = true;
                hq.1.clear();
                self.cond.notify_all();
                true
            }
            Err(e) => {
                // This error (a PoisonError) is probably the result of some earlier panic.
                // Don't panic again; just write the error to stderr.
                eprintln!("failed to acquire TaskQueue mutex: {:?}", e);
                false
            }
        }
    }
}

enum WorkerPhase {
    Init,
    Test,
    Halt,
}

struct PanicHandler<E: Debug + Send + 'static> {
    runner: Arc<Runner<E>>,
    phase: WorkerPhase,
}

impl<E: Debug + Send + 'static> Drop for PanicHandler<E> {
    fn drop(&mut self) {
        if thread::panicking() {
            match self.phase {
                // If the database initialization panicked, halt all tests.
                WorkerPhase::Init => {
                    self.runner.halt();
                }
                // If a test panicked, spawn a new worker.
                WorkerPhase::Test => Arc::clone(&self.runner).spawn_worker(),
                // If the tester is halted, there's nothing to clean up.
                WorkerPhase::Halt => (),
            }
        }
    }
}
