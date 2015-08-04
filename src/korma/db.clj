(ns korma.db
  "Functions for creating and managing database specifications."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.typed :as t :refer [ann defalias
                                              U I All TFn

                                              IFn Map


                                              Any Nilable HMap
                                              SequentialSeqable
                                              CountRange
                                              ]]
            [korma.types :refer [VPair HVPair IDontKnowYet Falsy Nameable ParameterizeLater]]))

(alias 'typed 'clojure.core.typed)

(defalias JdbcSpec (U

                    ;; Existing Connection
                    (HMap :mandatory {:connection java.sql.Connection})

                    ;; Factory
                    (HMap :mandatory {:factory [(HMap) -> java.sql.Connection]})

                    ;; DriverManager
                    (HMap :mandatory {:subprotocol String, :subname String}
                          :optional  {:classname String})

                    ;; DriverManager (alternative)
                    (HMap :mandatory {:dbtype String, :dbname String}
                          :optional  {:host String, :port Long})

                    ;; DataSource
                    (HMap :mandatory {:datasource javax.sql.DataSource})
                    (HMap :mandatory {:datasource javax.sql.DataSource
                                      :username String, :password String})
                    (HMap :mandatory {:datasource javax.sql.DataSource
                                      :user String, :password String})

                    ;; JNDI
                    (HMap :mandatory {:name (U String javax.naming.Name)}
                          :optional  {:environment java.util.Map})

                    ;; Raw
                    (HMap :mandatory {:connection-uri String})

                    java.net.URI

                    String))

(defalias ExternalSpec '"TODO")

(defalias KormaSpec (HMap :mandatory {:classname String
                                      :connection-uri String
                                      :naming #_NamingSpec '{:fields [IDontKnowYet -> IDontKnowYet]
                                                             :keys [String -> String]}
                                      :delimiters (t/SequentialSeqable ParameterizeLater)
                                      :alias-delimiter String}
                          :optional {:make-pool? Boolean
                                     :excess-timeout Integer
                                     :idle-timeout Integer
                                     :initial-pool-size Integer
                                     :minimum-pool-size Integer
                                     :maximum-pool-size Integer
                                     :test-connection-query (Nilable String)
                                     :idle-connection-test-period Integer
                                     :test-connection-on-checkin Boolean
                                     :test-connection-on-checkout Boolean}))

(defalias KormaConnection (HMap :mandatory {:pool (U (t/Delay #_ConnectionPool '{:datasource com.mchange.v2.c3p0.ComboPooledDataSource})
                                                     #_(t/Assoc KormaSpec ...)
                                                     #_spec
                                                     (HMap :mandatory {:classname String
                                                                       :connection-uri String
                                                                       :naming #_NamingSpec '{:fields [IDontKnowYet -> IDontKnowYet]
                                                                                              :keys [String -> String]}
                                                                       :delimiters (t/SequentialSeqable ParameterizeLater)
                                                                       :alias-delimiter String}
                                                           :optional {:make-pool? false
                                                                      :excess-timeout Integer
                                                                      :idle-timeout Integer
                                                                      :initial-pool-size Integer
                                                                      :minimum-pool-size Integer
                                                                      :maximum-pool-size Integer
                                                                      :test-connection-query (Nilable String)
                                                                      :idle-connection-test-period Integer
                                                                      :test-connection-on-checkin Boolean
                                                                      :test-connection-on-checkout Boolean}))
                                            :options #_DatabaseOptions '{:naming #_NamingSpec '{:fields [IDontKnowYet -> IDontKnowYet]
                                                                                                :keys [String -> String]}
                                                                         :delimiters (VPair Any) ; but probably should be something like `(U String Char)`
                                                                         :alias-delimiter #_String Any}
                                                     }
                                :optional {:rollback (t/Atom1 Boolean)}))
(ann _default (t/Atom1 (t/Nilable KormaConnection)))
(defonce _default (atom nil))

(ann *current-db* (t/Nilable (HMap :mandatory {;; :classname String
                                               ;; :connection-uri String
                                               ;; :naming NamingSpec
                                               ;; :delimiters (t/SequentialSeqable ParameterizeLater)
                                               ;; :alias-delimiter String
                                           ;; :options '{:naming '{:keys IDontKnowYet}}
                                               }
                                   :optional {;; :make-pool? false  ; CRUCIAL
                                              ;; :excess-timeout Integer
                                              ;; :idle-timeout Integer
                                              ;; :initial-pool-size Integer
                                              ;; :minimum-pool-size Integer
                                              ;; :maximum-pool-size Integer
                                              ;; :test-connection-query (Nilable String)
                                              ;; :idle-connection-test-period Integer
                                              ;; :test-connection-on-checkin Boolean
                                              ;; :test-connection-on-checkout Boolean
                                              })))
(def ^:dynamic *current-db* nil)


(ann *current-conn* (t/Nilable KormaConnection))
(def ^:dynamic *current-conn* nil)


(ann ->delimiters (All [x]
                    (t/IFn [Falsy                 -> (I (VPair (I String (t/ExactCount 1)))
                                                        (t/ExactCount 2))]
                           [(SequentialSeqable x) -> (HVPair (Nilable x) (Nilable x))])))
(defn- ->delimiters [delimiters]
  (if delimiters
    (let [[begin end] delimiters
          end (or end begin)]
      [begin end])
    ["\"" "\""]))

(defalias NamingSpec '{:fields [IDontKnowYet -> IDontKnowYet]
                       :keys [String -> String]})

(ann ->naming [(Nilable (HMap :optional {:fields [IDontKnowYet -> IDontKnowYet]
                                         :keys [String -> String]}))
               ->
               NamingSpec])
(defn- ->naming [strategy]
  (merge {:fields identity
          :keys identity} strategy))

(ann ->alias-delimiter (All [x]
                         (IFn [Falsy -> '" AS "]
                              [x -> x])))
(defn- ->alias-delimiter [alias-delimiter]
  (or alias-delimiter " AS "))

(defalias DatabaseOptions '{:naming NamingSpec
                            :delimiters (VPair Any) ; but probably should be something like `(U String Char)`
                            :alias-delimiter #_String Any})


(defalias FullyInstantiatedDatabaseSpecification (HMap :mandatory {:classname String
                                                                   :connection-uri String
                                                                   :naming NamingSpec
                                                                   :delimiters (t/SequentialSeqable ParameterizeLater)
                                                                   :alias-delimiter String ;; TODO (U y String)
                                                                   :make-pool? Boolean}
                                                       :optional {:encoding String
                                                                  :excess-timeout Integer
                                                                  :idle-timeout Integer
                                                                  :initial-pool-size Integer
                                                                  :minimum-pool-size Integer
                                                                  :maximum-pool-size Integer
                                                                  :test-connection-query (Nilable String)
                                                                  :idle-connection-test-period Integer
                                                                  :test-connection-on-checkin Boolean
                                                                  :test-connection-on-checkout Boolean}))


(defalias Spec FullyInstantiatedDatabaseSpecification)
#_(ann extract-options [Spec -> DatabaseOptions])

(ann extract-options (All [x y]
                       [(HMap :mandatory {:naming NamingSpec,
                                          :delimiters (SequentialSeqable x),
                                          :alias-delimiter (U String y)})
                        ->
                        (HMap :mandatory {:naming NamingSpec,
                                          :delimiters (HVPair (Nilable x) (Nilable x)),
                                          :alias-delimiter (U String y)})]))
#_(ann extract-options ['{:naming NamingSpec, :delimiters (SequentialSeqable )}])
(defn extract-options [{:keys [naming
                               delimiters
                               alias-delimiter]}]
  {:naming (->naming naming)
   :delimiters (->delimiters delimiters)
   :alias-delimiter (->alias-delimiter alias-delimiter)})

(ann default-connection [KormaConnection -> KormaConnection])
(defn default-connection
  "Set the database connection that Korma should use by default when no
  alternative is specified."
  [conn]
  (reset! _default conn))

(defalias SeqOfPairs (TFn [[k :variance :covariant]
                           [v :variance :covariant]]
                       (U (t/Map k v)
                          (t/Seq (HVPair k v)))))
(ann as-properties [(SeqOfPairs Nameable Any)
                    ->
                    java.util.Properties])
(defn- as-properties [m]
  (let [p (java.util.Properties.)]
    (typed/doseq [[k v] :- (HVPair Nameable Any), m]
      (.setProperty p (name k) (str v)))
    p))

(typed/def c3p0-enabled? :- Boolean
  (try
    (import 'com.mchange.v2.c3p0.ComboPooledDataSource)
    true
    (catch Throwable _ false)))

(defmacro resolve-new [class]
  (when-let [resolved (resolve class)]
    (t/print-env "resolve-new")
    `(new ~resolved)))

#_
(defalias SpecForCreateDB (TFn [[x :variance :covariant]
                                [y :variance :covariant]]
                            (t/Assoc (t/Assoc (DatabaseSpecification x y)
                                              ':delimiters (t/SequentialSeqable x))
                                     ':alias-delimiter (U y String))))
(defalias SpecForCreateDB (HMap :mandatory {:classname String
                                            :connection-uri String
                                            :naming NamingSpec
                                            :delimiters (t/SequentialSeqable ParameterizeLater)
                                            :alias-delimiter String ;; TODO (U y String)

                                            :make-pool? Boolean

                                            }
                                :optional {:excess-timeout Integer
                                           :idle-timeout Integer
                                           :initial-pool-size Integer
                                           :minimum-pool-size Integer
                                           :maximum-pool-size Integer
                                           :test-connection-query (Nilable String)
                                           :idle-connection-test-period Integer
                                           :test-connection-on-checkin Boolean
                                           :test-connection-on-checkout Boolean}))

(t/nilable-param com.mchange.v2.c3p0.ComboPooledDataSource/setPreferredTestQuery {1 #{0}})
(defalias ConnectionPool '{:datasource com.mchange.v2.c3p0.ComboPooledDataSource})
(defalias StrictConnectionPool (HMap :mandatory {:datasource com.mchange.v2.c3p0.ComboPooledDataSource}
                                     :complete? true))
(ann connection-pool [SpecForCreateDB -> StrictConnectionPool])
(defn connection-pool
  "Create a connection pool for the given database spec."
  [{:keys [connection-uri classname
           excess-timeout idle-timeout
           initial-pool-size minimum-pool-size maximum-pool-size
           test-connection-query
           idle-connection-test-period
           test-connection-on-checkin
           test-connection-on-checkout]
    :or {excess-timeout (* 30 60)
         idle-timeout (* 3 60 60)
         initial-pool-size 3
         minimum-pool-size 3
         maximum-pool-size 15
         test-connection-query nil
         idle-connection-test-period 0
         test-connection-on-checkin false
         test-connection-on-checkout false}
    :as spec}]
  {:datasource (doto (com.mchange.v2.c3p0.ComboPooledDataSource.)
                 (.setDriverClass classname)
                 (.setJdbcUrl connection-uri)
                 (.setProperties (do (t/print-env "at the beginng")
                                     (let [dissoced (dissoc spec
                                                            :make-pool? :classname :connection-uri
                                                            :naming :delimiters :alias-delimiter
                                                            :excess-timeout :idle-timeout
                                                            :initial-pool-size :minimum-pool-size :maximum-pool-size
                                                            :test-connection-query
                                                            :idle-connection-test-period
                                                            :test-connection-on-checkin
                                                            :test-connection-on-checkout)
                                           _ (t/print-env "after dissoc")
                                           _ (assert ((t/pred (SeqOfPairs Nameable Any))
                                                      dissoced))]
                                       (as-properties dissoced))
                                     #_(as-properties (dissoc spec
                                                        :make-pool? :classname :connection-uri
                                                        :naming :delimiters :alias-delimiter
                                                        :excess-timeout :idle-timeout
                                                        :initial-pool-size :minimum-pool-size :maximum-pool-size
                                                        :test-connection-query
                                                        :idle-connection-test-period
                                                        :test-connection-on-checkin
                                                        :test-connection-on-checkout))))
                 (.setMaxIdleTimeExcessConnections excess-timeout)
                 (.setMaxIdleTime idle-timeout)
                 (.setInitialPoolSize initial-pool-size)
                 (.setMinPoolSize minimum-pool-size)
                 (.setMaxPoolSize maximum-pool-size)
                 (.setIdleConnectionTestPeriod idle-connection-test-period)
                 (.setTestConnectionOnCheckin test-connection-on-checkin)
                 (.setTestConnectionOnCheckout test-connection-on-checkout)
                 (.setPreferredTestQuery test-connection-query))})


(ann delay-pool [SpecForCreateDB -> (t/Delay StrictConnectionPool)])
(defn delay-pool
  "Return a delay for creating a connection pool for the given spec."
  [spec]
  (delay (connection-pool spec)))

(ann ^:no-check
     get-connection (IFn [(HMap :mandatory {:pool (t/Delay ConnectionPool)}) -> ConnectionPool]

                         [KormaConnection -> KormaConnection]
                         #_
                         [IDontKnowYet -> IDontKnowYet]))
(defn get-connection
  "Get a connection from the potentially delayed connection object."
  [db]
  (let [db (or (:pool db) db)]
    (if-not db
      (throw (Exception. "No valid DB connection selected."))
      (do (t/print-env "before delay?")
          (if (delay? db)
              (do (t/print-env "yes delay?")
                  @db)
              (do (t/print-env "no delay?")
                  db))))))




(ann create-db (IFn [(HMap :mandatory {:classname String
                                       :connection-uri String
                                       :naming NamingSpec
                                       :delimiters (t/SequentialSeqable ParameterizeLater)
                                       :alias-delimiter String
                                       :make-pool? true  ; CRUCIAL
                                       }
                           :optional {:excess-timeout Integer
                                      :idle-timeout Integer
                                      :initial-pool-size Integer
                                      :minimum-pool-size Integer
                                      :maximum-pool-size Integer
                                      :test-connection-query (Nilable String)
                                      :idle-connection-test-period Integer
                                      :test-connection-on-checkin Boolean
                                      :test-connection-on-checkout Boolean})
                     ->
                     (HMap :mandatory {:pool (t/Delay ConnectionPool)
                                       :options DatabaseOptions}
                           :complete? true)]
                    [(HMap :mandatory {:classname String
                                       :connection-uri String
                                       :naming NamingSpec
                                       :delimiters (t/SequentialSeqable ParameterizeLater)
                                       :alias-delimiter String}
                           :optional {:make-pool? false  ; CRUCIAL
                                      :excess-timeout Integer
                                      :idle-timeout Integer
                                      :initial-pool-size Integer
                                      :minimum-pool-size Integer
                                      :maximum-pool-size Integer
                                      :test-connection-query (Nilable String)
                                      :idle-connection-test-period Integer
                                      :test-connection-on-checkin Boolean
                                      :test-connection-on-checkout Boolean})
                     ->
                     (HMap :mandatory {:pool (HMap :mandatory {:classname String
                                                               :connection-uri String
                                                               :naming NamingSpec
                                                               :delimiters (t/SequentialSeqable ParameterizeLater)
                                                               :alias-delimiter String}
                                                   :optional {:make-pool? false
                                                              :excess-timeout Integer
                                                              :idle-timeout Integer
                                                              :initial-pool-size Integer
                                                              :minimum-pool-size Integer
                                                              :maximum-pool-size Integer
                                                              :test-connection-query (Nilable String)
                                                              :idle-connection-test-period Integer
                                                              :test-connection-on-checkin Boolean
                                                              :test-connection-on-checkout Boolean})
                                       :options DatabaseOptions}
                           :complete? true)]))
(defn create-db
  "Create a db connection object manually instead of using defdb. This is often
   useful for creating connections dynamically, and probably should be followed
   up with:

   (default-connection my-new-conn)

   If the spec includes `:make-pool? true` makes a connection pool from the spec."
  [spec]
  {:pool (if (:make-pool? spec)
           (delay-pool spec)
           spec)
   :options (extract-options spec)})


(defmacro defdb
  "Define a database specification. The last evaluated defdb will be used by
  default for all queries where no database is specified by the entity."
  [db-name spec]
  `(let [spec# ~spec]
     (defonce ~db-name (create-db spec#))
     (default-connection ~db-name)))

(defalias StatedInputDBSpec (HMap :mandatory {:db String
                                              :user String
                                              :password String}
                                  :optional {:host String
                                             :port t/AnyInteger
                                             :make-pool? Boolean
                                             :delimiters IDontKnowYet}) )

(defalias ActualInputDBSpec (HMap :mandatory {:user String
                                              :password String}
                                  :optional {:db String
                                             :host String
                                             :port t/AnyInteger
                                             :make-pool? Boolean}))

(ann ^:no-check
     firebird [(HMap :mandatory {:user String
                                 :password String}
                     :optional {:db String
                                :host String
                                :port t/AnyInteger
                                :make-pool? Boolean})
               ->
               (HMap :mandatory {:classname String
                                 :connection-uri String
                                 :make-pool? Boolean})])
(defn firebird
  "Create a database specification for a FirebirdSQL database. Opts should include
  keys for :db, :user, :password. You can also optionally set host, port and make-pool?"
  [{:keys [host port db make-pool?]
    :or {host "localhost", port 3050, db "", make-pool? true}
    :as opts}]
  (merge {:classname "org.firebirdsql.jdbc.FBDriver" ; must be in classpath
          :connection-uri (str "jdbc:firebirdsql:" host "/" port ":" db)
          :make-pool? make-pool?
          :encoding "UTF8"}
         (dissoc opts :host :port :db)))

(ann postgres [(HMap) -> (HMap)])
(defn postgres
  "Create a database specification for a postgres database. Opts should include
  keys for :db, :user, and :password. You can also optionally set host and
  port."
  [{:keys [host port db make-pool?]
    :or {host "localhost", port 5432, db "", make-pool? true}
    :as opts}]
  (merge {:classname "org.postgresql.Driver" ; must be in classpath
          :connection-uri (str "jdbc:postgresql://" host ":" port "/" db)
          :make-pool? make-pool?}
         (dissoc opts :host :port :db)))

(ann oracle [(HMap) -> (HMap)])
(defn oracle
  "Create a database specification for an Oracle database. Opts should include keys
  for :user and :password. You can also optionally set host and port."
  [{:keys [host port make-pool?]
    :or {host "localhost", port 1521, make-pool? true}
    :as opts}]
  (merge {:classname "oracle.jdbc.driver.OracleDriver" ; must be in classpath
          :connection-uri (str "jdbc:oracle:thin:@" host ":" port)
          :make-pool? make-pool?}
         (dissoc opts :host :port)))

(ann mysql [(HMap) -> (HMap)])
(defn mysql
  "Create a database specification for a mysql database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port.
  Delimiters are automatically set to \"`\"."
  [{:keys [host port db make-pool?]
    :or {host "localhost", port 3306, db "", make-pool? true}
    :as opts}]
  (merge {:classname "com.mysql.jdbc.Driver" ; must be in classpath
          :connection-uri (str "jdbc:mysql://" host ":" port "/" db)
          :delimiters "`"
          :make-pool? make-pool?}
         (dissoc opts :host :port :db)))

(ann vertica [(HMap) -> (HMap)])
(defn vertica
  "Create a database specification for a vertica database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port.
  Delimiters are automatically set to \"`\"."
  [{:keys [host port db make-pool?]
    :or {host "localhost", port 5433, db "", make-pool? true}
    :as opts}]
  (merge {:classname "com.vertica.jdbc.Driver" ; must be in classpath
          :connection-uri (str "jdbc:vertica://" host ":" port "/" db)
          :delimiters "\""
          :make-pool? make-pool?}
         (dissoc opts :host :port :db)))

(ann mssql [(HMap) -> (HMap)])
(defn mssql
  "Create a database specification for a mssql database. Opts should include keys
  for :db, :user, and :password. You can also optionally set host and port."
  [{:keys [user password db host port make-pool?]
    :or {user "dbuser", password "dbpassword", db "", host "localhost", port 1433, make-pool? true}
    :as opts}]
  (merge {:classname "com.microsoft.sqlserver.jdbc.SQLServerDriver" ; must be in classpath
          :connection-uri (str "jdbc:sqlserver://" host ":" port ";database=" db ";user=" user ";password=" password)
          :make-pool? make-pool?}
         (dissoc opts :host :port :db)))

(ann ^:no-check msaccess [(HMap) -> (HMap)])
(defn msaccess
  "Create a database specification for a Microsoft Access database. Opts
  should include keys for :db and optionally :make-pool?."
  [{:keys [db make-pool?]
    :or {db "", make-pool? false}
    :as opts}]
  (merge {:classname "sun.jdbc.odbc.JdbcOdbcDriver" ; must be in classpath
          :connection-uri (str "jdbc:odbc:" (str "Driver={Microsoft Access Driver (*.mdb"
                                      (when (.endsWith db ".accdb") ", *.accdb")
                                      ")};Dbq=" db))
          :make-pool? make-pool?}
         (dissoc opts :db)))

(ann odbc [(HMap) -> (HMap)])
(defn odbc
  "Create a database specification for an ODBC DSN. Opts
  should include keys for :dsn and optionally :make-pool?."
  [{:keys [dsn make-pool?]
    :or {dsn "", make-pool? true}
    :as opts}]
  (merge {:classname "sun.jdbc.odbc.JdbcOdbcDriver" ; must be in classpath
          :connection-uri (str "jdbc:odbc:" dsn)
          :make-pool? make-pool?}
         (dissoc opts :dsn)))

(ann sqlite3 [(HMap) -> (HMap)])
(defn sqlite3
  "Create a database specification for a SQLite3 database. Opts should include a
  key for :db which is the path to the database file."
  [{:keys [db make-pool?]
    :or {db "sqlite.db", make-pool? true}
    :as opts}]
  (merge {:classname "org.sqlite.JDBC" ; must be in classpath
          :connection-uri (str "jdbc:sqlite:" db)
          :make-pool? make-pool?}
         (dissoc opts :db)))

(ann h2 [(HMap) -> (HMap)])
(defn h2
  "Create a database specification for a h2 database. Opts should include a key
  for :db which is the path to the database file."
  [{:keys [db make-pool?]
    :or {db "h2.db", make-pool? true}
    :as opts}]
  (merge {:classname "org.h2.Driver" ; must be in classpath
          :connection-uri (str "jdbc:h2:" db)
          :make-pool? make-pool?}
         (dissoc opts :db)))



(ann ^:no-check clojure.java.jdbc/get-connection [#_JdbcSpec (t/Map t/Any t/Any) -> java.sql.Connection])

(defmacro transaction
  "Execute all queries within the body in a single transaction.
  Optionally takes as a first argument a map to specify the :isolation and :read-only? properties of the transaction."
  {:arglists '([body] [options & body])}
  [& body]
  (let [options (first body)
        check-options (and (-> body rest seq)
                           (map? options))
        {:keys [isolation read-only?]} (when check-options options)
        body (if check-options (rest body) body)]
    `(binding [*current-db* (or *current-db* @_default)]
      (jdbc/with-db-transaction [conn# (or *current-conn* (get-connection *current-db*)) :isolation ~isolation :read-only? ~read-only?]
        (binding [*current-conn* conn#]
          ~@body)))))

(t/ann-protocol clojure.java.jdbc/Connectable
  add-connection [clojure.java.jdbc/Connectable
                  java.sql.Connection
                  ->
                  (clojure.lang.Associative t/Any t/Any)])

(ann ^:no-check clojure.java.jdbc/db-set-rollback-only! [Any -> true])
(ann ^:no-check clojure.java.jdbc/db-is-rollback-only [Any -> Boolean])
(comment

(ann clojure.java.jdbc/query [Any -> Any])
(ann clojure.java.jdbc/db-do-prepared-return-keys (IFn [;; #_(t/Difference DbSpec URI)  DbSpecWithoutURI
                                                        JdbcSpec
                                                        String
                                                        (t/Seqable Any)
                                                        ->
                                                        (t/Option (t/ASeq Any))  ; TODO pretty much a guess
                                                        ]
                                                       [;; #_DbSpecWithoutURI  DbSpecWithoutURI
                                                        JdbcSpec
                                                        Boolean
                                                        String
                                                        (t/Seqable Any)
                                                        ->
                                                        (t/Option (t/ASeq Any))  ; TODO pretty much a guess
                                                        ]))
(ann clojure.java.jdbc/db-do-prepared [Any -> Any]))


(ann rollback [-> true])
(defn rollback
  "Tell this current transaction to rollback."
  []
  (jdbc/db-set-rollback-only! *current-conn*))

(ann is-rollback? [-> Boolean])
(defn is-rollback?
  "Returns true if the current transaction will be rolled back"
  []
  (jdbc/db-is-rollback-only *current-conn*))

(defalias SomeInteger t/AnyInteger)
(defalias BatchResult (t/NilableNonEmptyASeq SomeInteger))

(ann ^:no-check
     exec-sql (IFn [(HMap :optional {:results ':results}) -> (t/ASeq (t/Map IDontKnowYet IDontKnowYet))]
                   [(HMap :optional {:results ':keys}) -> (t/Option (t/ASeq IDontKnowYet))]
                   [(HMap) -> BatchResult]))

(defn- exec-sql [{:keys [results sql-str params]}]
  (let [keys (get-in *current-db* [:options :naming :keys])]
    (case results
      :results (jdbc/query *current-conn*
                           (apply vector sql-str params)
                           :identifiers keys)
      :keys (jdbc/db-do-prepared-return-keys *current-conn* sql-str params)
      (jdbc/db-do-prepared *current-conn* sql-str params))))

(defmacro with-db
  "Execute all queries within the body using the given db spec"
  [db & body]
  `(jdbc/with-db-connection [conn# (korma.db/get-connection ~db)]
     (binding [*current-db* ~db
               *current-conn* conn#]
       ~@body)))

(ann ^:no-check
     do-query (IFn [(HMap :mandatory {:results ':results}
                          :optional {:db KormaConnection #_KormaSpec #_SpecForCreateDB})
                    ->
                    (t/ASeq (t/Map IDontKnowYet IDontKnowYet))]
                   [(HMap :mandatory {:results ':keys}
                          :optional {:db KormaConnection #_KormaSpec #_SpecForCreateDB})
                    ->
                    (t/Option (t/ASeq IDontKnowYet))]
                   [(HMap :optional {:db KormaConnection #_KormaSpec #_SpecForCreateDB})
                    ->
                    BatchResult]))
(defn do-query [{:keys [db] :as query}]
  (if *current-conn*
    (exec-sql query)
    (with-db (or db @_default)
      (exec-sql query))))
