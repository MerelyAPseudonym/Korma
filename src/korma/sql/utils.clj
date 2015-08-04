(ns ^{:no-doc true}
  korma.sql.utils
  (:require [clojure.string :as string]
            [korma.types :refer [IDontKnowYet Query]]
            [clojure.core.typed :as typed
                                :refer [ann ann-form All U Any
                                        HMap Option Seqable AVec
                                        NonEmptyASeq
                                        SequentialSeqable]]))

;;*****************************************************
;; map-types
;;*****************************************************
#_
(typed/defalias Generated (typed/TFn [[x :variance :covariant]]
                            (HMap :mandatory {::generated x})))
#_
(ann generated (All [x]
                 [x -> '{::generated x}]))
(ann generated [String -> '{::generated String}])
(defn generated [s]
  {::generated s})

#_
(ann sub-query (All [x]
                 [x -> '{::sub x}]))
(ann sub-query [Query -> '{::sub Query}])
(defn sub-query [s]
  {::sub s})

;; TODO do all pred functions ultimately return strings?
#_
(ann pred (All [a b d y]
            [[a b -> d] y -> '{::pred [a b -> d], ::args y}]))
(ann pred [[IDontKnowYet * -> String]
           (typed/Seqable IDontKnowYet)
           ->
           '{::pred [IDontKnowYet * -> String], ::args (typed/Seqable IDontKnowYet)}])
(defn pred [p args]
  {::pred p ::args args})

#_
(ann func (All [x y]
            [x y -> '{::func x, ::args y}]))
(ann func [String
           (typed/ASeq IDontKnowYet)
           ->
           '{::func String, ::args (typed/ASeq IDontKnowYet)}])
(defn func [f args]
  {::func f ::args args})

#_
(ann func? (All [x]
             (typed/IFn ['{::func x} -> x]
                        [Any -> nil])))
(ann func? (typed/IFn ['{::func String} -> String]
                      [Any -> nil]))
(defn func? [m]
  (::func m))

#_
(ann pred? (All [x]
             (typed/IFn ['{::pred x} -> x]
                        [Any -> nil])))
(ann pred? (typed/IFn ['{::pred [IDontKnowYet * -> String]} -> [IDontKnowYet * -> String]]
                      [Any -> nil]))
(defn pred? [m]
  (::pred m))

#_
(ann args? (All [x]
             (typed/IFn ['{::args x} -> x]
                        [Any -> nil])))
(ann args? (typed/IFn ['{::args (typed/Seqable IDontKnowYet)} -> (typed/Seqable IDontKnowYet)]
                      [Any -> nil]))
(defn args? [m]
  (::args m))

#_
(ann sub-query? (All [x]
                  (typed/IFn ['{::sub x} -> x]
                             [Any -> nil])))
(ann sub-query? (typed/IFn ['{::sub Query} -> Query]
                           [Any -> nil]))
(defn sub-query? [m]
  (::sub m))

#_
(ann generated? (All [x]
                  (typed/IFn ['{::generated x} -> x]
                             [Any -> nil])))
(ann generated? (typed/IFn ['{::generated String} -> String]
                           [Any -> nil]))
(defn generated? [m]
  (::generated m))


(typed/defn special-map? [m]
  (boolean (some (typed/fn [pred :- [Any -> Any]]
                   (pred m))
                 [func? pred? sub-query? generated?])))

;;*****************************************************
;; str-utils
;;*****************************************************
(ann comma-separated [(Option (Seqable Any)) -> String])
(defn comma-separated [vs]
  (string/join ", " vs))

(typed/defn wrap [v] :- String
  (str "(" v ")"))

(typed/defn :forall [x] left-assoc [vs :- (Option (SequentialSeqable x))] :- String
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

(typed/defn :forall [x y] vconcat
  [v1 :- (Option (Seqable x)),
   v2 :- (Option (Seqable y))] :- (AVec (U x y))
  (vec (concat v1 v2)))
