(ns korma.sql.engine

  (:require [clojure.core.typed :as t, :refer [All U I
                                               HMap Any]]
            [korma.types :refer [IDontKnowYet]]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [korma.sql.utils :as utils]
            [korma.db :as db]
            [korma.types :refer [Nameable IDontKnowYet]]))

;;*****************************************************
;; dynamic vars
;;*****************************************************
(t/defalias Table (U String  ;; TODO but maybe the (U String ...) should be in usages instead of in definition
                     (HMap :optional {:table IDontKnowYet
                                      :alias IDontKnowYet})))
(t/ann *bound-table* (t/Option Table))
(def ^{:dynamic true} *bound-table* nil)

(t/ann *bound-aliases* (t/Set IDontKnowYet))
(def ^{:dynamic true} *bound-aliases* #{})

(t/ann *bound-params* (t/Option IDontKnowYet))
(def ^{:dynamic true} *bound-params* nil)

(t/ann *bound-options* (t/Option (HMap :mandatory {:naming '{:fields [Any -> Any]}}
                                       :optional {:delimiters (t/SequentialSeqable Any)
                                                  :alias-delimiter #_String Any})))
#_(t/ann *bound-options (t/Option DatabaseOptions))
(def ^{:dynamic true} *bound-options* nil)

;;*****************************************************
;; delimiters
;;*****************************************************

(t/ann delimit-str [t/Any -> String])
(defn delimit-str [s]
  (let [{:keys [naming delimiters]} *bound-options*
        [begin end] delimiters
        ->field (:fields naming)
        _ (assert ->field)]
    (str begin (->field s) end)))

;;*****************************************************
;; Str utils
;;*****************************************************

(declare pred-map str-value)
(t/ann str-value [IDontKnowYet -> String])

(t/ann str-values [(t/Option (t/Seqable IDontKnowYet)) -> (t/ASeq String)])
(defn str-values [vs]
  (map str-value vs))

(t/ann comma-values [(t/Option (t/Seqable IDontKnowYet)) -> String])
(defn comma-values [vs]
  (utils/comma-separated (str-values vs)))

(t/ann wrap-values (t/IFn [nil -> '"(NULL)"]
                          #_
                          [(t/I (t/U (t/Coll Any)
                                     (t/Seqable Any))
                                t/EmptyCount)
                           ->
                           '"(NULL)"]
                          [(t/U (t/Coll Any)
                                (t/Seqable Any))
                           ->
                           String]))
(defn wrap-values [vs]
  (if (seq vs)
    (utils/wrap (comma-values vs))
    "(NULL)"))

(t/ann map-val (All [x]
                 (t/IFn ['{::utils/generated x} -> x]
                        [(HMap :mandatory {::utils/pred IDontKnowYet}
                               :optional {::utils/args (t/Seqable IDontKnowYet)})
                         ->
                         IDontKnowYet]
                        [(HMap :mandatory {::utils/func IDontKnowYet}
                               :optional {::utils/args (t/Seqable IDontKnowYet)})
                         ->
                         String]
                        ['{::utils/sub #_Query (HMap :optional {:params IDontKnowYet
                                                                :sql-str IDontKnowYet})}
                         ->
                         String]
                        [Any -> IDontKnowYet])))
(defn map-val [v]
  (let [func (utils/func? v)
        generated (utils/generated? v)
        args (utils/args? v)
        sub (utils/sub-query? v)
        pred (utils/pred? v)]
    (cond
      generated generated
      pred (apply pred args)
      func (let [vs (comma-values args)]
             (format func vs))
      sub (do
            (swap! *bound-params* utils/vconcat (:params sub))
            (utils/wrap (:sql-str sub)))
      :else (pred-map v))))

#_(t/ann table-alias [Entity -> ...])
(t/ann table-alias (All [x y]
                     [(HMap :optional {:table x
                                       :alias y})
                      ->
                      (U x y nil)]))
(defn table-alias [{:keys [table alias]}]
  (or alias table))

(t/ann table-identifier [String -> String])
(defn table-identifier [table-name]
  (let [parts (string/split table-name #"\.")]
    (if (next parts)
      (string/join "." (map delimit-str parts))
      (delimit-str table-name))))

(t/ann field-identifier (t/IFn [(t/Map Any Any) -> IDontKnowYet]
                               [Nameable -> String]))
(defn field-identifier [field]
  (cond
    (map? field) (map-val field)
    (string? field) field
    (= "*" (name field)) "*"
    :else (let [field-name (name field)
                parts (string/split field-name #"\.")]
            (if-not (next parts)
              (delimit-str field-name)
              (string/join "." (map delimit-str parts))))))


(t/ann prefix [Table
               (t/U (t/Map IDontKnowYet IDontKnowYet)
                    Nameable)
               ->
               IDontKnowYet])
(defn prefix [ent field]
  (let [field-name (field-identifier field)
        not-already-prefixed? (and (keyword? field)
                                   (not (*bound-aliases* field))
                                   (= -1 (.indexOf field-name ".")))]
    (if not-already-prefixed?
      (let [table (if (string? ent)
                    ent
                    (table-alias ent))]
        (str (table-identifier table) "." field-name))
      field-name)))

(t/ann try-prefix [IDontKnowYet -> IDontKnowYet])
(defn try-prefix [v]
  (let [bound-table #_@*bound-table* *bound-table*]
    (if (and (keyword? v)
             bound-table)
      (utils/generated (prefix bound-table v))
      v)))

(t/ann alias-clause [(t/Option Nameable) -> String])
(defn alias-clause [alias]
  (let [bound-options #_@*bound-options* *bound-options*]
    (when alias
      (str (:alias-delimiter bound-options)
           (delimit-str (name alias))))))

(t/ann field-str [(U Nameable (t/Map IDontKnowYet IDontKnowYet)) -> String])
(defn field-str [v]
  (let [bound-table #_@*bound-table* *bound-table*
        [fname alias] (if (vector? v)
                        v
                        [v nil])
        fname (cond
                (map? fname) (map-val fname)
                bound-table (prefix bound-table fname)
                :else (field-identifier fname))
        alias-str (alias-clause alias)]
    (str fname alias-str)))

(t/ann coll-str (t/IFn [nil -> '"(NULL)"]
                       #_
                           [(t/I (t/U (t/Coll Any)
                                      (t/Seqable Any))
                                 t/EmptyCount)
                            ->
                            '"(NULL)"]
                       [(t/U (t/Coll Any)
                             (t/Seqable Any))
                        ->
                        String]))
(defn coll-str [v]
  (wrap-values v))

(t/ann table-str (All [x]
                   (t/IFn [(U '{::utils/func Any} '{::utils/pred Any} '{::utils/sub Any} '{::utils/generated Any})
                           -> IDontKnowYet
                           ]
                          ;; any reason I shouldn't just (U ...) the two domains together?
                          [Nameable -> String]
                          ['{:table String} -> String])))
       (defn table-str [v]
         (if (utils/special-map? v)
    (map-val v)
    (let [tstr (cond
                 (string? v) v
                 (map? v) (:table v)
                 :else (name v))]
      (table-identifier tstr))))

(t/ann parameterize [IDontKnowYet -> '"?"])
(defn parameterize [v]
  (when *bound-params*
    (swap! *bound-params* conj v))
  "?")

(t/ann str-value (t/IFn [(t/Map IDontKnowYet IDontKnowYet) -> IDontKnowYet]
                        [t/Keyword -> String]
                        [nil -> '"NULL"]
                        #_[EmptyCollection -> '"(NULL)"]
                        [(t/Coll IDontKnowYet) -> String]
                        [Any -> IDontKnowYet]))
(defn str-value [v]
  (cond
    (map? v) (map-val v)
    (keyword? v) (field-str v)
    (nil? v) "NULL"
    (coll? v) (coll-str v)
    :else (parameterize v)))

(t/defn not-nil? [& vs]
  (every? #(not (nil? %)) vs))

;;*****************************************************
;; Bindings
;;*****************************************************
; query = '{:type Something, :table Something, :aliases Something, :db '{:options Something}
(defmacro bind-query [query & body]
  `(binding [*bound-table* (if (= :select (:type ~query))
                             (table-alias ~query)
                             (:table ~query))
             *bound-aliases* (or (:aliases ~query) #{})
             *bound-options* (or (get-in ~query [:db :options])
                                 (:options db/*current-db*)
                                 (:options @db/_default))]
     ~@body))

;;*****************************************************
;; Predicates
;;*****************************************************
(t/ann predicates (t/Map t/Symbol t/Symbol))
(def predicates {'like 'korma.sql.fns/pred-like
                 'and 'korma.sql.fns/pred-and
                 'or 'korma.sql.fns/pred-or
                 'not 'korma.sql.fns/pred-not
                 'in 'korma.sql.fns/pred-in
                 'exists 'korma.sql.fns/pred-exists
                 'not-in 'korma.sql.fns/pred-not-in
                 'between 'korma.sql.fns/pred-between
                 '> 'korma.sql.fns/pred->
                 '< 'korma.sql.fns/pred-<
                 '>= 'korma.sql.fns/pred->=
                 '<= 'korma.sql.fns/pred-<=
                 'not= 'korma.sql.fns/pred-not=
                 '= 'korma.sql.fns/pred-=})

(t/defalias UnaryOperator String)
(t/defalias InfixOperator String)
(t/defalias UnaryPredicate (U '"EXISTS" '"NOT"))
(t/defalias BinaryPredicate (U '"IN" '"NOT IN"
                               '">" '"<"
                               '">=" '"<="
                               '"<>"
                               '"LIKE" '"IS NOT"))

#_(t/ann do-infix [IDontKnowYet Any IDontKnowYet -> String])
(t/ann do-infix [IDontKnowYet String IDontKnowYet -> String])
(defn do-infix [k op v]
  (string/join " " [(str-value k) op (str-value v)]))

#_
(t/ann do-group [Any
                 (t/Option (t/Seqable Any))
                 ->
                 String])
(t/ann do-group [String
                 (t/Option (t/Seqable IDontKnowYet))
                 ->
                 String])
(defn do-group [op vs]
  (utils/wrap (string/join op (str-values vs))))

#_(t/ann do-wrapper [Any IDontKnowYet -> String])
(t/ann do-wrapper [String IDontKnowYet -> String])
(defn do-wrapper [op v]
  (str op (utils/wrap (str-value v))))

#_(t/ann do-trinary [IDontKnowYet Any IDontKnowYet Any IDontKnowYet -> String])
(t/ann do-trinary [IDontKnowYet String IDontKnowYet String IDontKnowYet -> String])
(defn do-trinary [k op v1 sep v2]
  (utils/wrap (string/join " " [(str-value k) op (str-value v1) sep (str-value v2)])))

#_
(t/ann trinary [IDontKnowYet Any IDontKnowYet Any IDontKnowYet
                ->
                '{::utils/pred [IDontKnowYet Any IDontKnowYet Any IDontKnowYet -> String]
                  ::utils/args (t/HSeq [IDontKnowYet Any IDontKnowYet Any IDontKnowYet])}])
(t/ann trinary [IDontKnowYet String IDontKnowYet String IDontKnowYet
                ->
                '{::utils/pred [IDontKnowYet String IDontKnowYet String IDontKnowYet -> String]
                  ::utils/args (t/HSeq [IDontKnowYet String IDontKnowYet String IDontKnowYet])}])
(defn trinary [k op v1 sep v2]
  (utils/pred do-trinary [(try-prefix k) op (try-prefix v1) sep (try-prefix v2)]))

#_
(t/ann infix [IDontKnowYet Any IDontKnowYet
              ->
              '{::utils/pred [IDontKnowYet Any IDontKnowYet -> String]
                ::utils/args (t/HSeq [IDontKnowYet Any IDontKnowYet])}])
(t/ann infix [IDontKnowYet String IDontKnowYet
              ->
              '{::utils/pred [IDontKnowYet String IDontKnowYet -> String]
                ::utils/args (t/HSeq [IDontKnowYet String IDontKnowYet])}])
(defn infix [k op v]
  (utils/pred do-infix [(try-prefix k) op (try-prefix v)]))

#_(t/ann group-with (All [x]))
(t/ann group-with [String (t/Seqable IDontKnowYet)
                   ->
                   '{::utils/pred [IDontKnowYet
                                   (t/Option (t/Seqable Any))
                                   ->
                                   String]
                     ::utils/args (t/HSeq [String (t/ASeq IDontKnowYet)])}])
(defn group-with [op vs]
  (utils/pred do-group [op (doall (map pred-map vs))]))

#_
(t/ann wrapper [Any IDontKnowYet
                ->
                '{::utils/pred [Any IDontKnowYet -> String]
                  ::utils/args (t/HSeq [Any IDontKnowYet])}])
(t/ann wrapper [String IDontKnowYet
                ->
                '{::utils/pred [String IDontKnowYet -> String]
                  ::utils/args (t/HSeq [String IDontKnowYet])}])
(defn wrapper [op v]
  (utils/pred do-wrapper [op v]))

(t/ann pred-and [Any * -> '{::utils/pred [IDontKnowYet
                                          (t/Option (t/Seqable Any))
                                          ->
                                          String]
                            ::utils/args (t/HSeq ['" AND " (t/ASeq IDontKnowYet)])}])
(defn pred-and [& args]
  (group-with " AND " args))

#_
(t/ann pred-= (All [x y]
                [(t/Option x) (t/Option y)
                 ->
                 '{::utils/pred [x String y -> String]
                   ::utils/args (t/HSeq [x String y])}]))
(t/ann pred-= (All [x y]
                (t/IFn [nil nil -> nil]
                       [x   nil -> '{::utils/pred [IDontKnowYet String IDontKnowYet -> String]
                                     ::utils/args (t/HSeq [x '"IS" nil])}]
                       [nil y   -> '{::utils/pred [IDontKnowYet String IDontKnowYet -> String]
                                     ::utils/args (t/HSeq [y '"IS" nil])}]
                       [x   y   -> '{::utils/pred [IDontKnowYet String IDontKnowYet]
                                     ::utils/args (t/HSeq [x '"=" y])}])))
(defn pred-= [k v]
  (cond
    (not-nil? k v) (infix k "=" v)
    (not-nil? k) (infix k "IS" v)
    (not-nil? v) (infix v "IS" k)))

(defn set= [[k v]]
  (map-val (infix k "=" v)))

(defn pred-vec [[k v]]
  (let [[func value] (if (vector? v)
                       v
                       [pred-= v])
        pred? (predicates func)
        func (if pred?
               (resolve pred?)
               func)]
    (func k value)))

(defn pred-map [m]
  (if (and (map? m)
           (not (utils/special-map? m)))
    (apply pred-and (doall (map pred-vec (sort-by (comp str key) m))))
    m))

(defn parse-where [form]
  (if (string? form)
    form
    (walk/postwalk-replace predicates form)))

;;*****************************************************
;; Aggregates
;;*****************************************************
(t/defalias DslAggregrate t/Symbol)
(t/defalias DslAggregrate' (t/U ''count ''min ''max ''first ''last ''avg ''stdev ''sum))
(t/defalias NamespaceQualifiedSymbol t/Symbol)
#_(t/ann aggregates (t/Map DslAggregrate NamespaceQualifiedSymbol))
(t/ann aggregates (t/Map t/Symbol t/Symbol))
(def aggregates {'count 'korma.sql.fns/agg-count
                 'min 'korma.sql.fns/agg-min
                 'max 'korma.sql.fns/agg-max
                 'first 'korma.sql.fns/agg-first
                 'last 'korma.sql.fns/agg-last
                 'avg 'korma.sql.fns/agg-avg
                 'stdev 'korma.sql.fns/agg-stdev
                 'sum 'korma.sql.fns/agg-sum})

(defn sql-func [op & vs]
  (utils/func (str (string/upper-case op) "(%s)")
              (map try-prefix vs)))

#_
    (comment
(defn parse-aggregate [form]
  (if (string? form)
    form
    (walk/postwalk-replace aggregates form)))

;;*****************************************************
;; Clauses
;;*****************************************************

(defn from-table [v & [already-aliased?]]
  (cond
    (string? v) (table-str v)
    (vector? v) (let [[table alias] v]
                  (str (from-table table :aliased) (alias-clause alias)))
    (map? v) (if (:table v)
               (let [{:keys [table alias]} v]
                 (str (table-str table) (when-not already-aliased? (alias-clause alias))))
               (map-val v))
    :else (table-str v)))

(defn join-clause [join-type table on-clause]
  (let [join-type (string/upper-case (name join-type))
        table (from-table table)
        join (str " " join-type " JOIN " table " ON ")]
    (str join (str-value on-clause))))

(defn insert-values-clause [ks vs]
  (for [v vs]
    (wrap-values (map #(get v %) ks))))

;;*****************************************************
;; Query types
;;*****************************************************

(defn sql-select [query]
  (let [clauses (map field-str (:fields query))
        modifiers-clause (when (seq (:modifiers query))
                           (str (reduce str (:modifiers query)) " "))
        clauses-str (utils/comma-separated clauses)
        neue-sql (str "SELECT " modifiers-clause clauses-str)]
    (assoc query :sql-str neue-sql)))

(defn sql-update [query]
  (let [neue-sql (str "UPDATE " (table-str query))]
    (assoc query :sql-str neue-sql)))

(defn sql-delete [query]
  (let [neue-sql (str "DELETE FROM " (table-str query))]
    (assoc query :sql-str neue-sql)))

(def noop-query "DO 0")

(defn sql-insert [query]
  (let [ins-keys (sort (keys (first (:values query))))
        keys-clause (utils/comma-separated (map field-identifier ins-keys))
        ins-values (insert-values-clause ins-keys (:values query))
        values-clause (utils/comma-separated ins-values)
        neue-sql (if-not (empty? ins-keys)
                   (str "INSERT INTO " (table-str query) " " (utils/wrap keys-clause) " VALUES " values-clause)
                   noop-query)]
    (assoc query :sql-str neue-sql)))

;;*****************************************************
;; Sql parts
;;*****************************************************

(defn sql-set [query]
  (let [fields (for [[k v] (sort (:set-fields query))]
                 [(utils/generated (field-identifier k)) (utils/generated (str-value v))])
        clauses (map set= fields)
        clauses-str (utils/comma-separated clauses)
        neue-sql (str " SET " clauses-str)]
    (update-in query [:sql-str] str neue-sql)))

(defn sql-joins [query]
  (let [clauses (for [[type table clause] (:joins query)]
                  (join-clause type table clause))
        tables (utils/comma-separated (map from-table (:from query)))
        clauses-str (utils/left-assoc (cons (str tables (first clauses))
                                            (rest clauses)))]
    (update-in query [:sql-str] str " FROM " clauses-str)))

(defn- sql-where-or-having [where-or-having-kw where-or-having-str query]
  (if (empty? (get query where-or-having-kw))
    query
    (let [clauses (map #(if (map? %) (map-val %) %)
                       (get query where-or-having-kw))
          clauses-str (string/join " AND " clauses)
          neue-sql (str where-or-having-str clauses-str)]
      (if (= "()" clauses-str)
        query
        (update-in query [:sql-str] str neue-sql)))))

(def sql-where  (partial sql-where-or-having :where  " WHERE "))
(def sql-having (partial sql-where-or-having :having " HAVING "))

(defn sql-order [query]
  (if (seq (:order query))
    (let [clauses (for [[k dir] (:order query)]
                    (str (str-value k) " " (string/upper-case (name dir))))
          clauses-str (utils/comma-separated clauses)
          neue-sql (str " ORDER BY " clauses-str)]
      (update-in query [:sql-str] str neue-sql))
    query))

(defn sql-group [query]
  (if (seq (:group query))
    (let [clauses (map field-str (:group query))
          clauses-str (utils/comma-separated clauses)
          neue-sql (str " GROUP BY " clauses-str)]
      (update-in query [:sql-str] str neue-sql))
    query))

(defn sql-limit-offset [{:keys [limit offset] :as query}]
  (let [limit-sql (when limit
                    (str " LIMIT " limit))
        offset-sql (when offset
                     (str " OFFSET " offset))]
    (update-in query [:sql-str] str limit-sql offset-sql)))

;;*****************************************************
;; Combination Queries
;;*****************************************************

(defn- sql-combination-query [type query]
  (let [sub-query-sqls (map map-val (:queries query))
        neue-sql (string/join (str " " type " ") sub-query-sqls)]
    (assoc query :sql-str neue-sql)))

(def sql-union     (partial sql-combination-query "UNION"))
(def sql-union-all (partial sql-combination-query "UNION ALL"))
(def sql-intersect (partial sql-combination-query "INTERSECT"))

;;*****************************************************
;; To sql
;;*****************************************************

(defmacro bind-params [& body]
  `(binding [*bound-params* (atom [])]
     (let [query# (do ~@body)]
       (update-in query# [:params] utils/vconcat @*bound-params*))))

(defn ->sql [query]
  (bind-params
   (case (:type query)
     :union (-> query sql-union sql-order)
     :union-all (-> query sql-union-all sql-order)
     :intersect (-> query sql-intersect sql-order)
     :select (-> query
                 sql-select
                 sql-joins
                 sql-where
                 sql-group
                 sql-having
                 sql-order
                 sql-limit-offset)
     :update (-> query
                 sql-update
                 sql-set
                 sql-where)
     :delete (-> query
                 sql-delete
                 sql-where)
     :insert (-> query
                 sql-insert)))))
