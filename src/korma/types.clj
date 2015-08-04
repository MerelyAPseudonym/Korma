(ns korma.types
  (:require [clojure.core.typed :as t :refer [defalias ann
                                              I U Any
                                              TFn
                                              ExactCount]]))

(defalias IDontKnowYet Any)

(defalias ParameterizeLater Any)

(defalias Query (HMap))

(defalias FlexPair
  "TODO"
  (TFn [[x :variance :covariant]
        [y :variance :covariant]]
    (t/HSequential [x y Any *])))

(defalias ProcessableAsPairs (TFn [[x :variance :covariant]
                                   [y :variance :covariant]]
                               (t/HSequential (FlexPair x y))))

;;;; Utility types (i.e., not Korma-specific)

(defalias HPair (TFn [[x :variance :covariant]
                      [y :variance :covariant]]
                  (t/HSequential [x y])))

(defalias VPair (TFn [[x :variance :covariant]]
                  (I (t/Vec x)
                     (ExactCount 2))))

(defalias HVPair (TFn [[k :variance :covariant]
                       [v :variance :covariant]]
                   (t/HVec [k v])))



(defalias Falsy (U nil false))
(defalias Falsey Falsy)  ; TODO pick a spelling

(defalias Nameable (U String clojure.lang.Named))
