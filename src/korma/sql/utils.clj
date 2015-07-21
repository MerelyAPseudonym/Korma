(ns ^{:no-doc true}
  korma.sql.utils
  (:require [clojure.string :as string]
            [clojure.core.typed :as typed
                                :refer [ann ann-form All U Any
                                        Option Seqable AVec
                                        NonEmptyASeq
                                        SequentialSeqable]]))

;;*****************************************************
;; map-types
;;*****************************************************
(ann generated (All [x]
                 [x -> '{::generated x}]))
(defn generated [s]
  {::generated s})

(ann sub-query (All [x]
                 [x -> '{::sub x}]))
(defn sub-query [s]
  {::sub s})

(ann pred (All [x y]
            [x y -> '{::pred x, ::args y}]))
(defn pred [p args]
  {::pred p ::args args})

(ann func (All [x y]
            [x y -> '{::func x, ::args y}]))
(defn func [f args]
  {::func f ::args args})


(typed/defn func? [m]
  (::func m))

(typed/defn pred? [m]
  (::pred m))

(typed/defn args? [m]
  (::args m))

(typed/defn sub-query? [m]
  (::sub m))

(typed/defn generated? [m]
  (::generated m))


(typed/defn special-map? [m]
  (boolean (some (typed/fn [pred :- [Any -> Any]]
                   (pred m))
                 [func? pred? sub-query? generated?])))

;;*****************************************************
;; str-utils
;;*****************************************************
(ann comma-separated (All [x]
                       [(Option (Seqable x)) -> String]))
(defn comma-separated [vs]
  (string/join ", " vs))

(typed/defn wrap [v] :- String
  (str "(" v ")"))

(typed/defn left-assoc :forall [x] [vs :- (Option (SequentialSeqable x))]
  (typed/loop [ret :- String, ""
               vs  :- (Option (SequentialSeqable x)), vs]
    (let [[v & vs] vs]
      (cond
        (nil? v) ret
        (nil? vs) (str ret v)
        :else (recur (wrap (str ret v)) vs)))))

;;*****************************************************
;; collection-utils
;;*****************************************************
(typed/defn vconcat :forall [x y]
  [v1 :- (Option (Seqable x)),
   v2 :- (Option (Seqable y))] :- (AVec (U x y))
  (vec (concat v1 v2)))
